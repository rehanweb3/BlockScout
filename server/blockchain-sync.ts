import { ethers } from "ethers";
import { storage } from "./storage";
import { broadcastNewBlock, broadcastNewTransaction } from "./websocket-broadcaster";
import WebSocket from "ws";
import dotenv from "dotenv";
import { cache } from "./cache";

dotenv.config();

interface BlockchainSyncConfig {
  rpcUrl: string;
  wssUrl: string;
  chainId: string;
  nativeToken: string;
}

const COMMON_ABIS = [
  // ERC20 & ERC721
  "function transfer(address to, uint256 value) public returns (bool)",
  "function approve(address spender, uint256 value) public returns (bool)",
  "function transferFrom(address from, address to, uint256 value) public returns (bool)",
  "function mint(address to, uint256 value) public",
  "function burn(address from, uint256 value) public",
  // Ownable
  "function transferOwnership(address newOwner) public",
  "function renounceOwnership() public",
  // Common Proxy/Admin
  "function upgradeTo(address newImplementation) public",
  "function initialize() public",
];

export class BlockchainSync {
  private formatError(error: any): string {
    if (!error) return String(error);
    try {
      if (typeof error === "string") return error;
      if (error instanceof Error) return error.stack || error.message;
      if (typeof (error as any).message === "string") return (error as any).message;
      return JSON.stringify(error);
    } catch (e) {
      return String(error);
    }
  }

  private provider: ethers.JsonRpcProvider;
  private wsProvider: ethers.WebSocketProvider | null = null;
  private config: BlockchainSyncConfig;
  private isRunning = false;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private blockProcessingQueue: Set<number> = new Set();

  constructor(config: BlockchainSyncConfig) {
    this.config = config;
    this.provider = new ethers.JsonRpcProvider(config.rpcUrl);
  }

  async start() {
    if (this.isRunning) {
      console.log("Blockchain sync already running");
      return;
    }

    this.isRunning = true;
    console.log("Starting blockchain sync service (v2 - Debug Mode)...");

    try {
      const syncState = await storage.getSyncState();
      const lastSyncedBlock = syncState?.lastSyncedBlock || 0;

      const currentBlock = await this.provider.getBlockNumber();
      console.log(`Current blockchain height: ${currentBlock}`);
      console.log(`Last synced block: ${lastSyncedBlock}`);

      if (lastSyncedBlock < currentBlock) {
        const blocksToSync = Math.min(currentBlock - lastSyncedBlock, 10);
        console.log(`Syncing ${blocksToSync} recent blocks...`);

        for (let i = currentBlock - blocksToSync + 1; i <= currentBlock; i++) {
          await this.processBlock(i);
        }
      }

      await this.connectWebSocket();
    } catch (error) {
      console.error("Error starting blockchain sync:", this.formatError(error));
      // Best-effort: don't let storage errors bubble to caller
      storage.updateSyncState(0, false).catch((e) => {
        console.error("Failed to update sync state after start error:", this.formatError(e));
      });
      this.scheduleReconnect();
    }
  }

  private async connectWebSocket() {
    try {
      console.log("Connecting to blockchain WebSocket...");
      this.wsProvider = new ethers.WebSocketProvider(
        this.config.wssUrl,
        Number(this.config.chainId)
      );
      this.wsProvider.on("block", async (blockNumber: number) => {
        console.log(`New block detected: ${blockNumber}`);
        await this.processBlock(blockNumber);
      });

      this.wsProvider.on("error", (error) => {
        console.error("WebSocket error:", this.formatError(error));
        this.handleDisconnect();
      });

      // The underlying websocket instance may not be exposed the same way across
      // ethers versions. Guard access to avoid TypeScript/runtime errors.
      // Try common fields that hold the raw WebSocket object.
      const rawWs: any = (this.wsProvider as any).ws || (this.wsProvider as any).websocket || (this.wsProvider as any)._websocket;
      if (rawWs && typeof rawWs.on === "function") {
        rawWs.on("close", () => {
          console.log("WebSocket connection closed");
          this.handleDisconnect();
        });
      } else if ((this.wsProvider as any).on) {
        // fallback: listen for 'close' on provider if supported
        try {
          (this.wsProvider as any).on("close", () => {
            console.log("WebSocket connection closed");
            this.handleDisconnect();
          });
        } catch (e) {
          // ignore
        }
      }

      await storage.updateSyncState(await this.provider.getBlockNumber(), true);
      console.log("WebSocket connected successfully");
    } catch (error) {
      console.error("WebSocket connection failed:", error);
      this.handleDisconnect();
    }
  }

  private handleDisconnect() {
    if (this.wsProvider) {
      try {
        this.wsProvider.destroy();
      } catch (e) {
      }
      this.wsProvider = null;
    }

    storage.updateSyncState(0, false).catch(console.error);
    this.scheduleReconnect();
  }

  private scheduleReconnect() {
    if (this.reconnectTimer) {
      return;
    }

    console.log("Scheduling WebSocket reconnect in 10 seconds...");
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.isRunning) {
        this.connectWebSocket();
      }
    }, 10000);
  }

  private async decodeFunctionData(data: string, contractAddress: string | null) {
    if (!data || data === "0x" || data.length < 10) return null;

    try {
      const methodId = data.slice(0, 10);
      let abi: any = COMMON_ABIS;

      // Try to get contract's specific ABI if verified
      if (contractAddress) {
        const contract = await storage.getContract(contractAddress);
        if (contract?.verified && contract.abi) {
          abi = contract.abi;
        }
      }

      const iface = new ethers.Interface(abi);
      const decoded = iface.parseTransaction({ data });

      if (decoded) {
        const args: Record<string, any> = {};
        decoded.fragment.inputs.forEach((input, i) => {
          const value = decoded.args[i];
          args[input.name || `arg${i}`] = typeof value === "bigint" ? value.toString() : value;
        });

        return {
          name: decoded.name,
          signature: decoded.signature,
          args,
          methodId,
        };
      }
    } catch (error) {
      // Internal decoding failure is fine, fall back to method ID
    }
    return null;
  }

  private async processBlock(blockNumber: number) {
    if (this.blockProcessingQueue.has(blockNumber)) {
      return;
    }

    this.blockProcessingQueue.add(blockNumber);

    try {
      const block = await this.provider.getBlock(blockNumber, true);
      if (!block) {
        console.warn(`Block ${blockNumber} not found`);
        return;
      }

      const existingBlock = await storage.getBlock(blockNumber);

      if (!existingBlock) {
        const baseFeePerGas = block.baseFeePerGas?.toString() || "0";
        const burntFees = block.baseFeePerGas
          ? (BigInt(block.gasUsed) * block.baseFeePerGas).toString()
          : "0";

        await storage.insertBlock({
          blockNumber: block.number,
          hash: block.hash || "",
          miner: block.miner || "",
          timestamp: block.timestamp,
          gasUsed: block.gasUsed.toString(),
          gasLimit: block.gasLimit.toString(),
          size: block.length || 0,
          baseFeePerGas,
          burntFees,
          transactionCount: block.transactions.length,
          parentHash: block.parentHash || null,
          extraData: block.extraData || null,
        });
      }

      // Always invalidate and broadcast if we reach this point (successfully fetched block data)
      console.log(`[Sync] Block ${block.number}: Ensuring server cache is fresh...`);
      await cache.invalidateBlockData();

      console.log(`[Sync] Block ${block.number}: Sending live update signal...`);
      broadcastNewBlock({
        blockNumber: block.number,
        hash: block.hash || "",
        miner: block.miner || "",
        timestamp: block.timestamp,
        transactionCount: block.transactions.length,
        gasUsed: block.gasUsed.toString(),
        gasLimit: block.gasLimit.toString(),
      });

      for (const txHash of block.transactions) {
        await this.processTransaction(txHash, block.number, block.timestamp);
      }

      await storage.updateSyncState(blockNumber, true);
      console.log(`Processed block ${blockNumber} with ${block.transactions.length} transactions`);
    } catch (error) {
      console.error(`Error processing block ${blockNumber}:`, error);
    } finally {
      this.blockProcessingQueue.delete(blockNumber);
    }
  }

  private async processTransaction(txHash: string | any, blockNumber: number, blockTimestamp: number) {
    try {
      // Handle case where txHash might be a full transaction object if provider.getBlock returns full transactions
      const validTxHash = typeof txHash === 'string' ? txHash : txHash?.hash;

      if (!validTxHash) {
        console.warn(`Debug: Invalid txHash encountered in block ${blockNumber}`, txHash);
        return;
      }

      console.log(`Debug: Fetching tx details for ${validTxHash}`);
      const [tx, receipt] = await Promise.all([
        this.provider.getTransaction(validTxHash),
        this.provider.getTransactionReceipt(validTxHash),
      ]);

      if (!tx || !receipt) {
        console.warn(`Debug: Failed to fetch tx or receipt for ${validTxHash}`);
        return;
      }

      const [isBlacklistedFrom, isBlacklistedTo] = await Promise.all([
        storage.isAddressBlacklisted(tx.from),
        tx.to ? storage.isAddressBlacklisted(tx.to) : Promise.resolve(false)
      ]);

      if (isBlacklistedFrom || isBlacklistedTo) {
        console.log(`Debug: Skipping transaction ${validTxHash} involving blacklisted address.`);
        return;
      }

      const existingTx = await storage.getTransaction(validTxHash);
      if (!existingTx) {
        console.log(`Debug: Inserting Transaction ${validTxHash} from ${tx.from} to ${tx.to}`);

        const contractCreated = receipt.contractAddress || null;
        const decodedInput = await this.decodeFunctionData(tx.data, tx.to);
        const method = decodedInput ? decodedInput.name : (tx.data.length > 10 ? tx.data.slice(0, 10) : null);

        // Special handling for transferOwnership logic details
        if (decodedInput?.name === "transferOwnership" && tx.to) {
          try {
            const previousOwner = await this.provider.call({
              to: tx.to,
              data: new ethers.Interface(["function owner() view returns (address)"]).encodeFunctionData("owner")
            }).then(res => ethers.AbiCoder.defaultAbiCoder().decode(["address"], res)[0]).catch(() => null);

            if (previousOwner) {
              decodedInput.args = {
                ...decodedInput.args,
                previousOwner,
                newOwner: decodedInput.args.newOwner
              };
            }
          } catch (e) {
            console.warn("Failed to fetch previous owner for transferOwnership");
          }
        }

        await storage.insertTransaction({
          txHash: tx.hash,
          blockNumber,
          fromAddress: tx.from,
          toAddress: tx.to || null,
          value: tx.value.toString(),
          gasPrice: tx.gasPrice?.toString() || "0",
          gasUsed: receipt.gasUsed.toString(),
          gasLimit: tx.gasLimit.toString(),
          status: receipt.status || 0,
          nonce: tx.nonce,
          transactionIndex: tx.index,
          input: tx.data,
          timestamp: blockTimestamp,
          method,
          contractCreated,
          decodedInput: decodedInput as any,
          logs: receipt.logs as any,
        });

        broadcastNewTransaction({
          txHash: tx.hash,
          blockNumber,
          fromAddress: tx.from,
          toAddress: tx.to || null,
          value: tx.value.toString(),
          status: receipt.status || 0,
        });

        console.log(`[Sync] Tx ${tx.hash}: Clearing cache for stats...`);
        await cache.invalidateBlockData(); // Also invalidate stats when tx arrives

        await this.updateAddress(tx.from, blockTimestamp);

        if (tx.to) {
          await this.updateAddress(tx.to, blockTimestamp);
        }

        if (contractCreated) {
          await this.updateAddress(contractCreated, blockTimestamp);

          await storage.upsertAddress({
            address: contractCreated,
            balance: "0",
            type: "Contract",
            isContract: true,
            firstSeen: blockTimestamp,
            lastSeen: blockTimestamp,
            transactionCount: 0,
          });

          await storage.insertContract({
            address: contractCreated,
            creator: tx.from,
            creationTxHash: txHash,
            sourceCode: null,
            compilerVersion: null,
            optimization: null,
            optimizationRuns: null,
            constructorArgs: null,
            abi: null,
            verified: false,
            verifiedAt: null,
            contractName: null,
          });
        }
      } else {
        console.log(`Debug: Transaction ${validTxHash} already exists. checking for token transfers...`);
      }

      for (const log of receipt.logs) {
        console.log(`Debug: Checking log for tx ${txHash}, address: ${log.address}, topics: ${log.topics}`);
        if (log.topics.length > 0 && log.topics[0] === "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef") {
          console.log(`Debug: Found Transfer event in tx ${txHash} for token ${log.address}`);
          const tokenAddress = log.address;

          const token = await storage.getToken(tokenAddress);
          if (!token) {
            console.log(`Debug: Token ${tokenAddress} not found in DB, indexing...`);
            await this.indexToken(tokenAddress);
          }

          // Process the transfer event to update holders and history
          console.log(`Debug: Processing token transfer for ${tokenAddress}`);
          await this.processTokenTransfer(log, blockNumber, blockTimestamp, txHash);
        }
      }
    } catch (error) {
      console.error(`Error processing transaction ${txHash}:`, error);
    }
  }

  private async updateAddress(address: string, timestamp: number) {
    try {
      const existing = await storage.getAddress(address);
      const balance = await this.provider.getBalance(address);

      if (!existing) {
        const code = await this.provider.getCode(address);
        const isContract = code !== "0x";

        await storage.upsertAddress({
          address,
          balance: balance.toString(),
          type: isContract ? "Contract" : "EOA",
          isContract,
          firstSeen: timestamp,
          lastSeen: timestamp,
          transactionCount: 1,
        });
        await cache.invalidateAddress(address);
      } else {
        await storage.upsertAddress({
          address,
          balance: balance.toString(),
          type: existing.type,
          isContract: existing.isContract,
          firstSeen: existing.firstSeen,
          lastSeen: timestamp,
          transactionCount: existing.transactionCount + 1,
        });
        await cache.invalidateAddress(address);
      }
    } catch (error) {
      console.error(`Error updating address ${address}:`, error);
    }
  }

  private async indexToken(tokenAddress: string) {
    try {
      const erc20Abi = [
        "function name() view returns (string)",
        "function symbol() view returns (string)",
        "function decimals() view returns (uint8)",
        "function totalSupply() view returns (uint256)",
      ];

      const contract = new ethers.Contract(tokenAddress, erc20Abi, this.provider);

      const [name, symbol, decimals, totalSupply] = await Promise.all([
        contract.name().catch(() => "Unknown"),
        contract.symbol().catch(() => "???"),
        contract.decimals().catch(() => 18),
        contract.totalSupply().catch(() => BigInt(0)),
      ]);

      await storage.upsertToken({
        address: tokenAddress,
        name,
        symbol,
        decimals: Number(decimals),
        totalSupply: totalSupply.toString(),
        logoUrl: null,
        logoStatus: "no_logo",
        submittedBy: null,
        submittedAt: null,
        reviewedAt: null,
        reviewedBy: null,
      });

      console.log(`Indexed token: ${symbol} (${name}) at ${tokenAddress}`);
    } catch (error) {
      console.error(`Error indexing token ${tokenAddress}:`, error);
    }
  }

  private async processTokenTransfer(log: any, blockNumber: number, timestamp: number, txHash: string) {
    try {
      const tokenAddress = log.address;
      // ERC20 Transfer event signature: Transfer(address indexed from, address indexed to, uint256 value)
      // topics[0] is hash, topics[1] is from, topics[2] is to
      if (log.topics.length < 3) return;

      const fromAddress = ethers.AbiCoder.defaultAbiCoder().decode(["address"], log.topics[1])[0];
      const toAddress = ethers.AbiCoder.defaultAbiCoder().decode(["address"], log.topics[2])[0];

      const [isBlacklistedFrom, isBlacklistedTo] = await Promise.all([
        storage.isAddressBlacklisted(fromAddress.toLowerCase()),
        storage.isAddressBlacklisted(toAddress.toLowerCase())
      ]);

      if (isBlacklistedFrom || isBlacklistedTo) {
        console.log(`Debug: Skipping token transfer in tx ${txHash} involving blacklisted address.`);
        return;
      }

      // Handle data (amount) - it might be empty or hex
      let amount = "0";
      try {
        if (log.data && log.data !== "0x") {
          amount = ethers.AbiCoder.defaultAbiCoder().decode(["uint256"], log.data)[0].toString();
        }
      } catch (e) {
        console.warn(`Failed to decode amount for tx ${txHash}`);
      }

      // Check if this transfer has already been indexed
      const exists = await storage.hasTokenTransfers(txHash);
      if (exists) {
        console.log(`Debug: Token transfers for ${txHash} already indexed. Skipping.`);
        return;
      }

      // 1. Insert Transfer Record
      await storage.insertTokenTransfer({
        tokenAddress,
        fromAddress,
        toAddress,
        value: amount,
        txHash,
        blockNumber,
        timestamp,
      });

      // 2. Update Token Holders (Fetch fresh balance from contract to be accurate)
      const tokenContract = new ethers.Contract(
        tokenAddress,
        ["function balanceOf(address) view returns (uint256)"],
        this.provider
      );

      // Update From Address
      if (fromAddress !== ethers.ZeroAddress) {
        try {
          const fromBalance = await tokenContract.balanceOf(fromAddress);
          await storage.upsertTokenHolder(tokenAddress, fromAddress, fromBalance.toString());
        } catch (e) {
          console.error(`Error updating balance for ${fromAddress}:`, e);
        }
      }

      // Update To Address
      if (toAddress !== ethers.ZeroAddress) {
        try {
          const toBalance = await tokenContract.balanceOf(toAddress);
          await storage.upsertTokenHolder(tokenAddress, toAddress, toBalance.toString());
        } catch (e) {
          console.error(`Error updating balance for ${toAddress}:`, e);
        }
      }

    } catch (error) {
      console.error(`Error processing token transfer in tx ${txHash}:`, error);
    }
  }

  stop() {
    console.log("Stopping blockchain sync service...");
    this.isRunning = false;

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    if (this.wsProvider) {
      this.wsProvider.destroy();
      this.wsProvider = null;
    }

    storage.updateSyncState(0, false).catch(console.error);
  }
}

let blockchainSync: BlockchainSync | null = null;

export function initBlockchainSync() {
  if (!process.env.RPC_URL || !process.env.WSS_URL) {
    console.warn("RPC_URL or WSS_URL not configured. Blockchain sync disabled.");
    return;
  }

  const config: BlockchainSyncConfig = {
    rpcUrl: process.env.RPC_URL,
    wssUrl: process.env.WSS_URL,
    chainId: process.env.CHAIN_ID || "1",
    nativeToken: process.env.VITE_NATIVE_TOKEN || "ATH",
  };

  blockchainSync = new BlockchainSync(config);
  blockchainSync.start().catch((error) => {
    console.error("Failed to start blockchain sync:", error);
  });

  return blockchainSync;
}

export function getBlockchainSync() {
  return blockchainSync;
}
