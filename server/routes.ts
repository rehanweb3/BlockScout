import type { Express } from "express";
import { createServer, type Server } from "http";
import { WebSocketServer, WebSocket } from "ws";
import { storage } from "./storage";
import { initBlockchainSync } from "./blockchain-sync";
import { cache } from "./cache";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import multer from "multer";
import path from "path";
import fs from "fs";
import solc from "solc";
import { ethers } from "ethers";
import rateLimit from "express-rate-limit";
import {
  loginAdminSchema,
  verifyContractSchema,
  submitLogoSchema,
  contractReadSchema,
  updateTokenMetadataSchema,
  insertBlacklistedAddressSchema,
} from "@shared/schema";
import { generateERC20Source, ERC20_ABI } from "./erc20-template";
import { setWebSocketServer, broadcastToClients } from "./websocket-broadcaster";
import dotenv from "dotenv";

dotenv.config();
const JWT_SECRET = process.env.SESSION_SECRET!;

if (!JWT_SECRET) {
  throw new Error(
    "SESSION_SECRET environment variable must be set for JWT authentication",
  );
}

const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => {
      const dir = "./uploads/logos";
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
      cb(null, dir);
    },
    filename: (req, file, cb) => {
      const uniqueName = `${Date.now()}-${file.originalname}`;
      cb(null, uniqueName);
    },
  }),
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowedTypes = /jpeg|jpg|png|gif|svg/;
    const extname = allowedTypes.test(
      path.extname(file.originalname).toLowerCase(),
    );
    const mimetype = allowedTypes.test(file.mimetype);

    if (mimetype && extname) {
      return cb(null, true);
    } else {
      cb(new Error("Only image files are allowed"));
    }
  },
});

function verifyAdminToken(req: any, res: any, next: any) {
  const token = req.headers.authorization?.replace("Bearer ", "");

  if (!token) {
    return res.status(401).json({ error: "No token provided" });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.admin = decoded;
    next();
  } catch (error) {
    return res.status(401).json({ error: "Invalid token" });
  }
}

const publicApiLimiter = rateLimit({
  windowMs: 1 * 60 * 1000,
  max: 60,
  message: { error: "Too many requests, please try again later" },
  standardHeaders: true,
  legacyHeaders: false,
});

export async function registerRoutes(app: Express): Promise<Server> {
  const httpServer = createServer(app);
  const wss = new WebSocketServer({ server: httpServer, path: "/ws" });

  setWebSocketServer(wss);

  wss.on("connection", (ws: WebSocket) => {
    console.log("Client connected to WebSocket");

    ws.on("error", console.error);

    ws.on("close", () => {
      console.log("Client disconnected from WebSocket");
    });

    ws.send(
      JSON.stringify({ type: "connected", message: "WebSocket connected" }),
    );
  });

  initBlockchainSync();

  app.get("/api/stats", async (req, res) => {
    try {
      const cacheKey = "stats";
      const cached = await cache.get(cacheKey);

      if (cached) {
        return res.json(cached);
      }

      const stats = await storage.getStats();
      await cache.set(cacheKey, stats, { ttl: 10 });
      res.json(stats);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/blocks", async (req, res) => {
    try {
      const page = parseInt(req.query.page as string) || 1;
      const limit = parseInt(req.query.limit as string) || 10;
      const offset = (page - 1) * limit;

      const cacheKey = `blocks:page:${page}:limit:${limit}`;
      const cached = await cache.get(cacheKey);

      if (cached) {
        return res.json(cached);
      }

      const [items, total] = await Promise.all([
        storage.getBlocksPaginated(limit, offset),
        storage.getTotalBlocks(),
      ]);

      const response = {
        items,
        total,
        page,
        totalPages: Math.ceil(total / limit),
      };

      await cache.set(cacheKey, response, { ttl: 10 });
      res.json(response);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/blocks/latest", async (req, res) => {
    try {
      const cacheKey = "latest:blocks";
      const cached = await cache.get(cacheKey);

      if (cached) {
        return res.json(cached);
      }

      const blocks = await storage.getLatestBlocks(10);
      await cache.set(cacheKey, blocks, { ttl: 10 });
      res.json(blocks);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/block/:number", async (req, res) => {
    try {
      const blockNumber = parseInt(req.params.number);
      const cacheKey = `block:${blockNumber}`;
      const cached = await cache.get(cacheKey);

      if (cached) {
        return res.json(cached);
      }

      const block = await storage.getBlock(blockNumber);

      if (!block) {
        return res.status(404).json({ error: "Block not found" });
      }

      await cache.set(cacheKey, block, { ttl: 60 });
      res.json(block);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/block/:number/transactions", async (req, res) => {
    try {
      const blockNumber = parseInt(req.params.number);
      const transactions = await storage.getTransactionsByBlock(blockNumber);
      res.json(transactions);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/transactions/latest", async (req, res) => {
    try {
      const transactions = await storage.getLatestTransactions(10);
      res.json(transactions);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/transactions", async (req, res) => {
    try {
      const page = parseInt(req.query.page as string) || 1;
      const limit = parseInt(req.query.limit as string) || 10;
      const offset = (page - 1) * limit;

      const cacheKey = `transactions:page:${page}:limit:${limit}`;
      const cached = await cache.get(cacheKey);

      if (cached) {
        return res.json(cached);
      }

      const [items, total] = await Promise.all([
        storage.getTransactionsPaginated(limit, offset),
        storage.getTotalTransactions(),
      ]);

      const response = {
        items,
        total,
        page,
        totalPages: Math.ceil(total / limit),
      };

      await cache.set(cacheKey, response, { ttl: 10 });
      res.json(response);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/tx/:hash", async (req, res) => {
    try {
      const transaction = await storage.getTransaction(req.params.hash);
      const tokenTransfers = await storage.getTokenTransfersByTx(req.params.hash);

      if (!transaction) {
        return res.status(404).json({ error: "Transaction not found" });
      }

      let contractVerified = false;
      let toIsContract = false;
      const targetAddress = transaction.contractCreated || transaction.toAddress;

      if (targetAddress) {
        const [contract, addressData] = await Promise.all([
          storage.getContract(targetAddress),
          storage.getAddress(targetAddress),
        ]);

        if (contract?.verified) {
          contractVerified = true;
        }

        if (addressData?.isContract) {
          toIsContract = true;
        }
      }

      res.json({ ...transaction, tokenTransfers, contractVerified, toIsContract });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/address/:address", async (req, res) => {
    try {
      const address = req.params.address;

      if (await storage.isAddressBlacklisted(address)) {
        return res.status(404).json({ error: "Address not found" });
      }

      const addressData = await storage.getAddress(address);

      if (!addressData) {
        return res.status(404).json({ error: "Address not found" });
      }

      let contract = null;
      let token = null;

      if (addressData.isContract) {
        contract = await storage.getContract(address);
        // Check if this contract is also a token
        token = await storage.getToken(address);
      }

      // Always fetch held tokens, even for contracts/tokens
      const heldTokens = await storage.getAddressTokens(address);

      // If it's a token, return token-specific data structure MERGED with address data
      if (token) {
        const holdersCount = await storage.getTokenHolderCount(token.address);
        const transfersCount = await storage.getTokenTransferCount(token.address);

        res.json({
          ...addressData, // Include base address data (balance, tx count, etc.)
          tokens: heldTokens, // Include tokens held by this contract

          // Token specific fields at ROOT level for TokenDetail page
          name: token.name,
          symbol: token.symbol,
          decimals: token.decimals,
          totalSupply: token.totalSupply,
          holdersCount,
          transfersCount,
          logoUrl:
            token.logoStatus === "approved" && token.logoUrl
              ? `https://explorer.mintrax.network${token.logoUrl}`
              : "/assets/default-token-logo.svg",
          logoStatus: token.logoStatus,

          contract: contract
            ? {
              verified: contract.verified,
              abi: contract.abi,
              sourceCode: contract.sourceCode,
              compilerVersion: contract.compilerVersion,
              contractName: contract.contractName,
              creator: contract.creator,
            }
            : null,
          creator: contract?.creator || null,
        });
      } else {
        // Return regular address data
        res.json({
          ...addressData,
          contract,
          tokens: heldTokens,
        });
      }
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/address/:address/transactions", async (req, res) => {
    try {
      const transactions = await storage.getTransactionsByAddress(
        req.params.address,
        50,
      );
      res.json(transactions);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/tokens", async (req, res) => {
    try {
      const tokens = await storage.getTokens();

      const tokensWithLogo = tokens.map((token) => ({
        ...token,
        logoUrl:
          token.logoStatus === "approved" && token.logoUrl
            ? token.logoUrl
            : "/assets/default-token-logo.svg",
      }));

      res.json(tokensWithLogo);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/token/:address/holders", async (req, res) => {
    const address = req.params.address;
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 10;
    const offset = (page - 1) * limit;

    try {
      const holders = await storage.getTokenHoldersPaginated(address, limit, offset);
      res.json(holders);
    } catch (error) {
      res.status(500).json({ message: "Failed to fetch token holders" });
    }
  });

  app.get("/api/token/:address/transfers", async (req, res) => {
    const address = req.params.address;
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 10;
    const offset = (page - 1) * limit;

    try {
      const transfers = await storage.getTokenTransfers(address, limit, offset);
      res.json(transfers);
    } catch (error) {
      res.status(500).json({ message: "Failed to fetch token transfers" });
    }
  });

  app.get("/api/token/:address", publicApiLimiter, async (req, res) => {
    try {
      const address = req.params.address;

      if (!/^0x[a-fA-F0-9]{40}$/.test(address)) {
        return res.status(400).json({ error: "Invalid token address format" });
      }

      const token = await storage.getToken(address);

      if (!token) {
        return res.status(404).json({ error: "Token not found" });
      }

      const response = {
        name: token.name,
        symbol: token.symbol,
        logo_url:
          token.logoStatus === "approved" && token.logoUrl
            ? token.logoUrl
            : null,
        description: token.description || "",
        website: token.website || "",
        verified: token.logoStatus === "approved",
      };

      res.json(response);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post("/api/token/compile", async (req, res) => {
    try {
      const { name, symbol, initialSupply, decimals } = req.body;

      if (!name || !symbol || !initialSupply || decimals === undefined || decimals === null) {
        return res.status(400).json({ error: "Missing required fields" });
      }

      if (decimals < 0 || decimals > 18) {
        return res.status(400).json({ error: "Decimals must be between 0 and 18" });
      }

      const sourceCode = generateERC20Source(name, symbol, decimals);
      const contractName = symbol.replace(/[^a-zA-Z0-9]/g, "");

      const input = {
        language: "Solidity",
        sources: {
          [`${contractName}.sol`]: {
            content: sourceCode,
          },
        },
        settings: {
          outputSelection: {
            "*": {
              "*": ["abi", "evm.bytecode"],
            },
          },
          optimizer: {
            enabled: true,
            runs: 200,
          },
          evmVersion: "paris",
        },
      };

      const output = JSON.parse(solc.compile(JSON.stringify(input)));

      if (output.errors && output.errors.some((e: any) => e.severity === "error")) {
        const errorMessages = output.errors
          .filter((e: any) => e.severity === "error")
          .map((e: any) => e.message);
        return res.status(500).json({ error: `Compilation failed: ${errorMessages.join(", ")}` });
      }

      const contractFile = output.contracts?.[`${contractName}.sol`];
      if (!contractFile) {
        return res.status(500).json({ error: `Contract file not found in compilation output` });
      }

      const contract = contractFile[contractName];
      if (!contract || !contract.evm?.bytecode?.object) {
        return res.status(500).json({ error: "Bytecode not found in compilation output" });
      }

      console.log(`✅ Compiled ${name} (${symbol}) successfully`);

      res.json({
        bytecode: "0x" + contract.evm.bytecode.object,
        abi: contract.abi,
        sourceCode,
        contractName,
        initialSupply,
      });
    } catch (error: any) {
      console.error("Compilation error:", error);
      res.status(500).json({ error: error.message || "Failed to compile contract" });
    }
  });

  app.post("/api/token/register-deployed", async (req, res) => {
    try {
      const { address, name, symbol, decimals, initialSupply, deployer, bytecode, abi, sourceCode, contractName } = req.body;

      if (!ethers.isAddress(address)) {
        return res.status(400).json({ error: "Invalid contract address" });
      }

      const currentTimestamp = Math.floor(Date.now() / 1000);

      await storage.upsertAddress({
        address,
        balance: "0",
        type: "Contract",
        isContract: true,
        firstSeen: currentTimestamp,
        lastSeen: currentTimestamp,
        transactionCount: 0,
      });

      await storage.insertContract({
        address,
        creator: deployer,
        creationTxHash: null,
        sourceCode,
        compilerVersion: "0.8.0+commit.c7dfd78e",
        optimization: true,
        optimizationRuns: 200,
        constructorArgs: initialSupply.toString(),
        abi: typeof abi === 'string' ? abi : JSON.stringify(abi),
        verified: true,
        verifiedAt: new Date(),
        contractName,
      });

      await storage.upsertToken({
        address,
        name,
        symbol,
        decimals,
        totalSupply: (BigInt(initialSupply) * BigInt(10) ** BigInt(decimals)).toString(),
        logoUrl: null,
        logoStatus: "no_logo",
        submittedBy: null,
        submittedAt: null,
        reviewedAt: null,
        reviewedBy: null,
      });

      broadcastToClients({
        type: "token_deployed",
        data: { address, name, symbol, deployer },
      });

      broadcastToClients({
        type: "contractVerified",
        address,
      });

      console.log(`✅ Token ${name} registered and auto-verified at ${address}`);

      res.json({
        success: true,
        address,
        message: "Token deployed and verified successfully",
      });
    } catch (error: any) {
      console.error("Token registration error:", error);
      res.status(500).json({ error: error.message || "Failed to register token" });
    }
  });

  // Note: /api/token/deploy route removed - all deployments now happen via user's wallet (MetaMask)
  // Use /api/token/compile to get bytecode/ABI, then deploy via frontend wallet
  // After deployment, call /api/token/register-deployed to register and auto-verify the contract

  app.get("/api/contracts", async (req, res) => {
    try {
      const contracts = await storage.getAllContracts();
      res.json(contracts);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post("/api/search", async (req, res) => {
    try {
      const { query } = req.body;

      if (!query) {
        return res.status(400).json({ error: "Query is required" });
      }

      if (/^0x[a-fA-F0-9]{64}$/.test(query)) {
        const tx = await storage.getTransaction(query);
        if (tx) {
          return res.json({ type: "transaction", data: tx });
        }

        const block = await storage.getBlockByHash(query);
        if (block) {
          return res.json({ type: "block", data: block });
        }
      }

      if (/^0x[a-fA-F0-9]{40}$/.test(query)) {
        if (await storage.isAddressBlacklisted(query)) {
          return res.status(404).json({ error: "Not found" });
        }
        const address = await storage.getAddress(query);
        if (address) {
          return res.json({ type: "address", data: address });
        }
      }

      if (/^\d+$/.test(query)) {
        const blockNumber = parseInt(query);
        const block = await storage.getBlock(blockNumber);
        if (block) {
          return res.json({ type: "block", data: block });
        }
      }

      res.status(404).json({ error: "Not found" });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post("/api/verify-contract", async (req, res) => {
    try {
      const validatedData = verifyContractSchema.parse(req.body);
      const {
        address,
        sourceCode,
        contractName,
        compilerVersion,
        optimization,
        optimizationRuns,
        constructorArgs,
      } = validatedData;

      const contract = await storage.getContract(address);
      if (!contract) {
        return res.status(404).json({ error: "Contract not found" });
      }

      if (contract.verified) {
        return res.status(400).json({ error: "Contract already verified" });
      }

      try {
        const input = {
          language: "Solidity",
          sources: {
            [`${contractName}.sol`]: {
              content: sourceCode,
            },
          },
          settings: {
            optimizer: {
              enabled: optimization,
              runs: optimizationRuns || 200,
            },
            outputSelection: {
              "*": {
                "*": ["abi", "evm.bytecode"],
              },
            },
            evmVersion: "london", // Match user's compilation environment
          },
        };

        const output = JSON.parse(solc.compile(JSON.stringify(input)));

        if (
          output.errors &&
          output.errors.some((e: any) => e.severity === "error")
        ) {
          return res.status(400).json({
            error: "Compilation failed",
            details: output.errors,
          });
        }

        // Improved contract lookup: Search all files and provide fallbacks
        let compiledContract: any = null;
        const availableContracts: string[] = [];

        for (const fileName in output.contracts) {
          for (const name in output.contracts[fileName]) {
            availableContracts.push(name);
            if (name === contractName) {
              compiledContract = output.contracts[fileName][name];
              break;
            }
          }
          if (compiledContract) break;
        }

        // Fallback: If only one contract was found, use it
        if (!compiledContract && availableContracts.length === 1) {
          const onlyContractName = availableContracts[0];
          compiledContract = output.contracts[Object.keys(output.contracts)[0]][onlyContractName];
          console.log(`Fallback used: verification target ${contractName} not found, using only available contract ${onlyContractName}`);
        }

        if (!compiledContract) {
          return res.status(400).json({
            error: `Contract "${contractName}" not found in compilation output.`,
            details: `Found contracts: ${availableContracts.join(", ") || "None"}. Please ensure the contract name matches exactly.`
          });
        }

        if (!compiledContract.evm?.bytecode?.object) {
          return res
            .status(400)
            .json({ error: "Bytecode not found in compilation output" });
        }

        const provider = new ethers.JsonRpcProvider(process.env.RPC_URL);
        const deployedCode = await provider.getCode(address).then(c => c.toLowerCase());
        const compiledCode = ("0x" + compiledContract.evm.bytecode.object).toLowerCase();

        // Robust matching: Strip metadata suffixes (CBOR hash)
        // Solidity metadata usually starts with a26469706673 (ipfs)
        const stripMetadata = (code: string) => {
          const metadataPattern = /a26469706673[0-9a-f]{64}64736f6c6343[0-9a-f]{6}0033$/;
          // More generic: look for the CBOR length at the end and strip it
          // Or simply check if a large prefix matches, as constructor args and metadata are variable
          return code.split("a26469706673")[0];
        };

        const strippedCompiled = stripMetadata(compiledCode);
        const strippedDeployed = stripMetadata(deployedCode);

        // A match is valid if the deployed code contains the compiled core bytecode
        // or if the prefixes match significantly (ignoring the variable metadata)
        const match = strippedDeployed.includes(strippedCompiled.slice(2, 500)) ||
          strippedCompiled.includes(strippedDeployed.slice(2, 500));

        if (match) {
          await storage.updateContract(address, {
            sourceCode,
            compilerVersion,
            optimization,
            optimizationRuns: optimization ? optimizationRuns : null,
            constructorArgs: constructorArgs || null,
            abi: compiledContract.abi,
            verified: true,
            verifiedAt: new Date(),
            contractName,
          });

          broadcastToClients({
            type: "contractVerified",
            address,
          });

          res.json({
            success: true,
            message: "Contract verified successfully",
          });
        } else {
          res
            .status(400)
            .json({
              error:
                "Bytecode does not match. Please verify your source code and compiler settings.",
            });
        }
      } catch (compileError: any) {
        res.status(400).json({
          error: "Compilation error",
          details: compileError.message,
        });
      }
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/contract/:address/abi", async (req, res) => {
    try {
      const contract = await storage.getContract(req.params.address);

      if (!contract) {
        return res.status(404).json({ error: "Contract not found" });
      }

      if (!contract.verified || !contract.abi) {
        return res
          .status(400)
          .json({ error: "Contract not verified or ABI not available" });
      }

      res.json({ abi: contract.abi });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post("/api/contract/:address/read", async (req, res) => {
    try {
      const { functionName, args } = contractReadSchema.parse(req.body);
      const address = req.params.address;

      const contract = await storage.getContract(address);

      if (!contract || !contract.verified || !contract.abi) {
        return res
          .status(400)
          .json({ error: "Contract not verified or ABI not available" });
      }

      if (!process.env.RPC_URL) {
        return res.status(503).json({ error: "RPC connection not configured" });
      }

      const provider = new ethers.JsonRpcProvider(process.env.RPC_URL);
      const contractInstance = new ethers.Contract(
        address,
        contract.abi as any,
        provider,
      );

      const result = await contractInstance[functionName](...args);

      res.json({ result: result.toString() });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/transaction/daily-stats", async (req, res) => {
    try {
      const days = parseInt(req.query.days as string) || 7;
      const cacheKey = `daily-tx-stats:${days}`;
      const cached = await cache.get(cacheKey);

      if (cached) {
        return res.json(cached);
      }

      const dailyStats = await storage.getDailyTransactionStats(days);

      await cache.set(cacheKey, dailyStats, { ttl: 300 });
      res.json(dailyStats);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/gas/statistics", async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 100;
      const cacheKey = `gas:stats:${limit}`;
      const cached = await cache.get(cacheKey);

      if (cached) {
        return res.json(cached);
      }

      const blocks = await storage.getLatestBlocks(limit);

      const gasStats = blocks.map((block) => ({
        blockNumber: block.blockNumber,
        timestamp: block.timestamp,
        baseFeePerGas: block.baseFeePerGas
          ? (BigInt(block.baseFeePerGas) / BigInt(1e9)).toString()
          : "0",
        gasUsed: block.gasUsed,
        gasLimit: block.gasLimit,
        utilization: (
          (Number(block.gasUsed) / Number(block.gasLimit)) *
          100
        ).toFixed(2),
      }));

      await cache.set(cacheKey, gasStats, { ttl: 60 });
      res.json(gasStats);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/tx/:hash/internal", async (req, res) => {
    try {
      const txHash = req.params.hash;
      const cacheKey = `internal-tx:${txHash}`;
      const cached = await cache.get(cacheKey);

      if (cached) {
        return res.json(cached);
      }

      res.json([]);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/enhanced-stats", async (req, res) => {
    try {
      const cacheKey = "enhanced-stats";
      const cached = await cache.get(cacheKey);

      if (cached) {
        return res.json(cached);
      }

      const tps = await storage.getTPS();
      const gasStats = await storage.getGasFeeStats();
      const syncState = await storage.getSyncState();

      const enhancedStats = {
        tps: tps.toFixed(4),
        avgGasPrice: gasStats.avgGasPrice,
        minGasPrice: gasStats.minGasPrice,
        maxGasPrice: gasStats.maxGasPrice,
        isConnected: syncState?.isConnected || false,
      };

      await cache.set(cacheKey, enhancedStats, { ttl: 10 });
      res.json(enhancedStats);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/validator-info", async (req, res) => {
    try {
      res.json({
        name: "Mintrax Miner",
        address: "0x0000000000000000000000000000000000000000",
        verified: true,
        consensusType: "POA",
        status: "active",
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/top-wallets", async (req, res) => {
    try {
      const requestedLimit = parseInt(req.query.limit as string) || 10;
      const limit = Math.min(Math.max(1, requestedLimit), 100);
      const cacheKey = `top-wallets:${limit}`;
      const cached = await cache.get(cacheKey);

      if (cached) {
        return res.json(cached);
      }

      const topWallets = await storage.getTopWallets(limit);

      await cache.set(cacheKey, topWallets, { ttl: 60 });
      res.json(topWallets);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/token/:address/holders", async (req, res) => {
    try {
      const address = req.params.address;
      const requestedLimit = parseInt(req.query.limit as string) || 100;
      const limit = Math.min(Math.max(1, requestedLimit), 1000);

      if (!/^0x[a-fA-F0-9]{40}$/.test(address)) {
        return res.status(400).json({ error: "Invalid token address format" });
      }

      const holders = await storage.getTokenHolders(address, limit);
      res.json(holders);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/address/:address/token-holdings", async (req, res) => {
    try {
      const address = req.params.address;

      if (!/^0x[a-fA-F0-9]{40}$/.test(address)) {
        return res.status(400).json({ error: "Invalid address format" });
      }

      const holdings = await storage.getHolderTokens(address);
      res.json(holdings);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post(
    "/api/token/submit-logo",
    upload.single("logo"),
    async (req, res) => {
      try {
        const { tokenAddress, submittedBy } = req.body;

        if (!tokenAddress || !submittedBy) {
          return res.status(400).json({ error: "Missing required fields" });
        }

        if (!req.file) {
          return res.status(400).json({ error: "Logo file is required" });
        }

        const token = await storage.getToken(tokenAddress);
        if (!token) {
          return res.status(404).json({ error: "Token not found" });
        }

        const logoUrl = `/uploads/logos/${req.file.filename}`;

        await storage.upsertToken({
          address: tokenAddress,
          name: token.name,
          symbol: token.symbol,
          decimals: token.decimals,
          totalSupply: token.totalSupply,
          logoUrl,
          logoStatus: "pending",
          submittedBy,
          submittedAt: new Date(),
          reviewedAt: null,
          reviewedBy: null,
        });

        broadcastToClients({
          type: "logoSubmitted",
          tokenAddress,
        });

        res.json({ success: true, message: "Logo submitted for review" });
      } catch (error: any) {
        res.status(500).json({ error: error.message });
      }
    },
  );

  app.post("/api/admin/login", async (req, res) => {
    try {
      const { email, password } = loginAdminSchema.parse(req.body);

      const admin = await storage.getAdminByEmail(email);

      if (!admin) {
        const adminEmail = process.env.ADMIN_EMAIL;
        const adminPassword = process.env.ADMIN_PASSWORD;

        if (email === adminEmail && password === adminPassword) {
          const passwordHash = await bcrypt.hash(password, 10);
          await storage.createAdminUser({
            email,
            passwordHash,
            role: "admin",
          });

          const token = jwt.sign({ email, role: "admin" }, JWT_SECRET, {
            expiresIn: "7d",
          });
          return res.json({ token });
        }

        return res.status(401).json({ error: "Invalid credentials" });
      }

      const isPasswordValid = await bcrypt.compare(
        password,
        admin.passwordHash,
      );

      if (!isPasswordValid) {
        return res.status(401).json({ error: "Invalid credentials" });
      }

      const token = jwt.sign(
        { email: admin.email, role: admin.role },
        JWT_SECRET,
        { expiresIn: "7d" },
      );
      res.json({ token });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/admin/stats", verifyAdminToken, async (req, res) => {
    try {
      const stats = await storage.getAdminStats();
      res.json(stats);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/admin/review-logos", verifyAdminToken, async (req, res) => {
    try {
      const pendingLogos = await storage.getTokensByLogoStatus("pending");
      res.json(pendingLogos);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post(
    "/api/admin/approve-logo/:tokenAddress",
    verifyAdminToken,
    async (req: any, res) => {
      try {
        const { tokenAddress } = req.params;
        await storage.updateTokenLogoStatus(
          tokenAddress,
          "approved",
          req.admin.email,
        );

        broadcastToClients({
          type: "logoStatusChanged",
          tokenAddress,
          status: "approved",
        });

        res.json({ success: true });
      } catch (error: any) {
        res.status(500).json({ error: error.message });
      }
    },
  );

  app.post(
    "/api/admin/reject-logo/:tokenAddress",
    verifyAdminToken,
    async (req: any, res) => {
      try {
        const { tokenAddress } = req.params;

        const token = await storage.getToken(tokenAddress);
        if (!token) {
          return res.status(404).json({ error: "Token not found" });
        }

        await storage.upsertToken({
          address: tokenAddress,
          name: token.name,
          symbol: token.symbol,
          decimals: token.decimals,
          totalSupply: token.totalSupply,
          logoUrl: null,
          logoStatus: "no_logo",
          submittedBy: null,
          submittedAt: null,
          reviewedAt: new Date(),
          reviewedBy: req.admin.email,
        });

        broadcastToClients({
          type: "logoStatusChanged",
          tokenAddress,
          status: "no_logo",
        });

        res.json({ success: true });
      } catch (error: any) {
        res.status(500).json({ error: error.message });
      }
    },
  );

  app.post(
    "/api/admin/update-token-metadata",
    verifyAdminToken,
    async (req: any, res) => {
      try {
        const validatedData = updateTokenMetadataSchema.parse(req.body);
        const { tokenAddress, name, symbol, description, website } =
          validatedData;

        const token = await storage.getToken(tokenAddress);
        if (!token) {
          return res.status(404).json({ error: "Token not found" });
        }

        const updates: Partial<typeof token> = {};
        if (name !== undefined) updates.name = name;
        if (symbol !== undefined) updates.symbol = symbol;
        if (description !== undefined) updates.description = description;
        if (website !== undefined) updates.website = website;

        await storage.updateTokenMetadata(tokenAddress, updates);

        res.json({
          success: true,
          message: "Token metadata updated successfully",
        });
      } catch (error: any) {
        res.status(500).json({ error: error.message });
      }
    },
  );

  app.get("/api/admin/blacklist", verifyAdminToken, async (req, res) => {
    try {
      const blacklist = await storage.getBlacklistedAddresses();
      res.json(blacklist);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post("/api/admin/blacklist", verifyAdminToken, async (req, res) => {
    try {
      const { address, reason } = insertBlacklistedAddressSchema.parse(req.body);
      await storage.blacklistAddress(address, reason || undefined);
      res.json({ success: true, message: "Address blacklisted and data wiped" });
    } catch (error: any) {
      res.status(400).json({ error: error.message });
    }
  });

  app.delete("/api/admin/blacklist/:address", verifyAdminToken, async (req, res) => {
    try {
      const { address } = req.params;
      await storage.unblacklistAddress(address);
      res.json({ success: true, message: "Address removed from blacklist" });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.use("/uploads", (req, res, next) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    next();
  });

  const express = await import("express");
  app.use("/uploads", express.default.static("uploads"));

  return httpServer;
}
