import { db } from "./db";
import { eq, desc, and, or, sql, asc } from "drizzle-orm";
import * as schema from "@shared/schema";
import type {
  Block, InsertBlock,
  Transaction, InsertTransaction,
  Address, InsertAddress,
  Contract, InsertContract,
  Token, InsertToken,
  TokenTransfer, InsertTokenTransfer,
  AdminUser, InsertAdminUser,
  SyncState,
  TokenHolder, InsertTokenHolder,
  BlacklistedAddress, InsertBlacklistedAddress
} from "@shared/schema";

export interface IStorage {
  getLatestBlock(): Promise<number>;
  getSyncState(): Promise<SyncState | undefined>;
  updateSyncState(blockNumber: number, isConnected: boolean): Promise<void>;

  insertBlock(block: InsertBlock & { blockNumber: number }): Promise<Block>;
  getBlock(blockNumber: number): Promise<Block | undefined>;
  getBlockByHash(hash: string): Promise<Block | undefined>;
  getLatestBlocks(limit: number): Promise<Block[]>;
  getBlocksPaginated(limit: number, offset: number): Promise<Block[]>;
  getTotalBlocks(): Promise<number>;

  insertTransaction(tx: InsertTransaction & { txHash: string }): Promise<Transaction>;
  getTransaction(txHash: string): Promise<Transaction | undefined>;
  getLatestTransactions(limit: number): Promise<Transaction[]>;
  getTransactionsPaginated(limit: number, offset: number): Promise<Transaction[]>;
  getTransactionsByBlock(blockNumber: number): Promise<Transaction[]>;
  getTransactionsByAddress(address: string, limit: number): Promise<Transaction[]>;
  getTotalTransactions(): Promise<number>;

  upsertAddress(address: InsertAddress & { address: string }): Promise<Address>;
  getAddress(address: string): Promise<Address | undefined>;
  updateAddressBalance(address: string, balance: string): Promise<void>;
  getTotalAddresses(): Promise<number>;

  insertContract(contract: InsertContract & { address: string }): Promise<Contract>;
  updateContract(address: string, updates: Partial<Contract>): Promise<void>;
  getContract(address: string): Promise<Contract | undefined>;
  getVerifiedContracts(): Promise<Contract[]>;
  getAllContracts(): Promise<any[]>;
  getTotalVerifiedContracts(): Promise<number>;

  upsertToken(token: InsertToken & { address: string }): Promise<Token>;
  getToken(address: string): Promise<Token | undefined>;
  getTokens(): Promise<Token[]>;
  getTokensByLogoStatus(status: string): Promise<Token[]>;
  updateTokenLogoStatus(address: string, status: string, reviewedBy: string): Promise<void>;
  updateTokenMetadata(address: string, updates: Partial<Token>): Promise<void>;

  insertTokenTransfer(transfer: InsertTokenTransfer): Promise<TokenTransfer>;

  createAdminUser(admin: InsertAdminUser): Promise<AdminUser>;
  getAdminByEmail(email: string): Promise<AdminUser | undefined>;

  getStats(): Promise<{
    latestBlock: number;
    totalTransactions: number;
    totalAddresses: number;
    totalContracts: number;
    isConnected: boolean;
  }>;

  getAdminStats(): Promise<{
    pendingLogos: number;
    approvedLogos: number;
    rejectedLogos: number;
    totalBlocks: number;
    totalTransactions: number;
  }>;

  getDailyTransactionStats(days: number): Promise<Array<{
    date: string;
    count: number;
    totalValue: string;
  }>>;

  getTopWallets(limit: number): Promise<Array<{
    address: string;
    balance: string;
    type: string;
    transactionCount: number;
  }>>;

  getTPS(): Promise<number>;

  getGasFeeStats(): Promise<{
    avgGasPrice: string;
    minGasPrice: string;
    maxGasPrice: string;
  }>;

  getTokenHolders(tokenAddress: string, limit: number): Promise<TokenHolder[]>;
  upsertTokenHolder(tokenAddress: string, holderAddress: string, balance: string): Promise<TokenHolder>;
  getHolderTokens(holderAddress: string): Promise<Array<{
    tokenAddress: string;
    balance: string;
    token: Token | null;
  }>>;

  getTokenHolderCount(tokenAddress: string): Promise<number>;
  getTokenTransferCount(tokenAddress: string): Promise<number>;
  hasTokenTransfers(txHash: string): Promise<boolean>;
  getTokenTransfers(tokenAddress: string, limit: number, offset: number): Promise<TokenTransfer[]>;
  getTokenTransfers(tokenAddress: string, limit: number, offset: number): Promise<TokenTransfer[]>;
  getTokenHoldersPaginated(tokenAddress: string, limit: number, offset: number): Promise<TokenHolder[]>;
  getAddressTokens(address: string): Promise<any[]>;
  getTokenTransfersByTx(txHash: string): Promise<any[]>;

  // Blacklist methods
  blacklistAddress(address: string, reason?: string): Promise<void>;
  unblacklistAddress(address: string): Promise<void>;
  getBlacklistedAddresses(): Promise<BlacklistedAddress[]>;
  isAddressBlacklisted(address: string): Promise<boolean>;
}

export class DatabaseStorage implements IStorage {
  async getLatestBlock(): Promise<number> {
    const result = await db
      .select({ blockNumber: schema.blocks.blockNumber })
      .from(schema.blocks)
      .orderBy(desc(schema.blocks.blockNumber))
      .limit(1);

    return result[0]?.blockNumber || 0;
  }

  async getSyncState(): Promise<SyncState | undefined> {
    const [state] = await db.select().from(schema.syncState).limit(1);
    return state;
  }

  async updateSyncState(blockNumber: number, isConnected: boolean): Promise<void> {
    await db
      .insert(schema.syncState)
      .values({
        id: 1,
        lastSyncedBlock: blockNumber,
        lastSyncedAt: new Date(),
        isConnected,
      })
      .onConflictDoUpdate({
        target: schema.syncState.id,
        set: {
          lastSyncedBlock: blockNumber,
          lastSyncedAt: new Date(),
          isConnected,
        },
      });
  }

  async insertBlock(block: InsertBlock & { blockNumber: number }): Promise<Block> {
    const [inserted] = await db
      .insert(schema.blocks)
      .values(block)
      .onConflictDoNothing()
      .returning();
    return inserted;
  }

  async getBlock(blockNumber: number): Promise<Block | undefined> {
    const [block] = await db
      .select()
      .from(schema.blocks)
      .where(eq(schema.blocks.blockNumber, blockNumber));
    return block;
  }

  async getBlockByHash(hash: string): Promise<Block | undefined> {
    const [block] = await db
      .select()
      .from(schema.blocks)
      .where(eq(schema.blocks.hash, hash));
    return block;
  }

  async getLatestBlocks(limit: number): Promise<Block[]> {
    return db
      .select()
      .from(schema.blocks)
      .orderBy(desc(schema.blocks.blockNumber))
      .limit(limit);
  }

  async getBlocksPaginated(limit: number, offset: number): Promise<Block[]> {
    return db
      .select()
      .from(schema.blocks)
      .orderBy(desc(schema.blocks.blockNumber))
      .limit(limit)
      .offset(offset);
  }

  async getTotalBlocks(): Promise<number> {
    const [result] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.blocks);
    return result?.count || 0;
  }

  async insertTransaction(tx: InsertTransaction & { txHash: string }): Promise<Transaction> {
    const [inserted] = await db
      .insert(schema.transactions)
      .values(tx)
      .onConflictDoNothing()
      .returning();
    return inserted;
  }

  async getTransaction(txHash: string): Promise<Transaction | undefined> {
    const [tx] = await db
      .select()
      .from(schema.transactions)
      .where(eq(schema.transactions.txHash, txHash));
    return tx;
  }

  async getLatestTransactions(limit: number): Promise<Transaction[]> {
    return db
      .select()
      .from(schema.transactions)
      .orderBy(desc(schema.transactions.timestamp))
      .limit(limit);
  }

  async getTransactionsPaginated(limit: number, offset: number): Promise<Transaction[]> {
    return db
      .select()
      .from(schema.transactions)
      .orderBy(desc(schema.transactions.timestamp))
      .limit(limit)
      .offset(offset);
  }

  async getTotalTransactions(): Promise<number> {
    const [result] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.transactions);
    return result?.count || 0;
  }

  async getTransactionsByBlock(blockNumber: number): Promise<Transaction[]> {
    return db
      .select()
      .from(schema.transactions)
      .where(eq(schema.transactions.blockNumber, blockNumber))
      .orderBy(schema.transactions.transactionIndex);
  }

  async getTransactionsByAddress(address: string, limit: number): Promise<Transaction[]> {
    return db
      .select()
      .from(schema.transactions)
      .where(
        or(
          eq(schema.transactions.fromAddress, address),
          eq(schema.transactions.toAddress, address)
        )
      )
      .orderBy(desc(schema.transactions.timestamp))
      .limit(limit);
  }

  async upsertAddress(address: InsertAddress & { address: string }): Promise<Address> {
    const [upserted] = await db
      .insert(schema.addresses)
      .values(address)
      .onConflictDoUpdate({
        target: schema.addresses.address,
        set: {
          balance: address.balance,
          lastSeen: address.lastSeen,
          transactionCount: sql`${schema.addresses.transactionCount} + 1`,
        },
      })
      .returning();
    return upserted;
  }

  async getAddress(address: string): Promise<Address | undefined> {
    const [addr] = await db
      .select()
      .from(schema.addresses)
      .where(eq(schema.addresses.address, address));
    return addr;
  }

  async updateAddressBalance(address: string, balance: string): Promise<void> {
    await db
      .update(schema.addresses)
      .set({ balance })
      .where(eq(schema.addresses.address, address));
  }

  async getTotalAddresses(): Promise<number> {
    const [result] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.addresses);
    return result?.count || 0;
  }

  async insertContract(contract: InsertContract & { address: string }): Promise<Contract> {
    const [inserted] = await db
      .insert(schema.contracts)
      .values(contract)
      .onConflictDoUpdate({
        target: schema.contracts.address,
        set: {
          sourceCode: contract.sourceCode,
          compilerVersion: contract.compilerVersion,
          optimization: contract.optimization,
          optimizationRuns: contract.optimizationRuns,
          constructorArgs: contract.constructorArgs,
          abi: contract.abi,
          verified: contract.verified,
          verifiedAt: contract.verifiedAt,
          contractName: contract.contractName,
        },
      })
      .returning();
    return inserted;
  }

  async updateContract(address: string, updates: Partial<Contract>): Promise<void> {
    await db
      .update(schema.contracts)
      .set(updates)
      .where(eq(schema.contracts.address, address));
  }

  async getContract(address: string): Promise<Contract | undefined> {
    const [contract] = await db
      .select()
      .from(schema.contracts)
      .where(eq(schema.contracts.address, address));
    return contract;
  }

  async getVerifiedContracts(): Promise<Contract[]> {
    return db
      .select()
      .from(schema.contracts)
      .where(eq(schema.contracts.verified, true))
      .orderBy(desc(schema.contracts.verifiedAt));
  }

  async getAllContracts(): Promise<any[]> {
    return db
      .select({
        address: schema.addresses.address,
        creator: schema.contracts.creator,
        contractName: schema.contracts.contractName,
        verified: schema.contracts.verified,
        verifiedAt: schema.contracts.verifiedAt,
        compilerVersion: schema.contracts.compilerVersion,
        optimization: schema.contracts.optimization,
        optimizationRuns: schema.contracts.optimizationRuns,
        firstSeen: schema.addresses.firstSeen,
      })
      .from(schema.addresses)
      .leftJoin(schema.contracts, eq(schema.addresses.address, schema.contracts.address))
      .where(eq(schema.addresses.isContract, true))
      .orderBy(desc(schema.addresses.firstSeen));
  }

  async getTotalVerifiedContracts(): Promise<number> {
    const [result] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.contracts)
      .where(eq(schema.contracts.verified, true));
    return result?.count || 0;
  }

  async upsertToken(token: InsertToken & { address: string }): Promise<Token> {
    const [upserted] = await db
      .insert(schema.tokens)
      .values(token)
      .onConflictDoUpdate({
        target: schema.tokens.address,
        set: {
          name: token.name,
          symbol: token.symbol,
          decimals: token.decimals,
          totalSupply: token.totalSupply,
          logoUrl: token.logoUrl,
          logoStatus: token.logoStatus,
          submittedBy: token.submittedBy,
          submittedAt: token.submittedAt,
          reviewedAt: token.reviewedAt,
          reviewedBy: token.reviewedBy,
        },
      })
      .returning();
    return upserted;
  }

  async getToken(address: string): Promise<Token | undefined> {
    const [token] = await db
      .select()
      .from(schema.tokens)
      .where(eq(schema.tokens.address, address));
    return token;
  }

  async getTokens(): Promise<Token[]> {
    return db
      .select()
      .from(schema.tokens)
      .orderBy(schema.tokens.symbol);
  }

  async getTokensByLogoStatus(status: string): Promise<Token[]> {
    return db
      .select()
      .from(schema.tokens)
      .where(eq(schema.tokens.logoStatus, status))
      .orderBy(desc(schema.tokens.submittedAt));
  }

  async updateTokenLogoStatus(address: string, status: string, reviewedBy: string): Promise<void> {
    await db
      .update(schema.tokens)
      .set({
        logoStatus: status,
        reviewedAt: new Date(),
        reviewedBy,
      })
      .where(eq(schema.tokens.address, address));
  }

  async updateTokenMetadata(address: string, updates: Partial<Token>): Promise<void> {
    await db
      .update(schema.tokens)
      .set(updates)
      .where(eq(schema.tokens.address, address));
  }

  async insertTokenTransfer(transfer: InsertTokenTransfer): Promise<TokenTransfer> {
    const [inserted] = await db
      .insert(schema.tokenTransfers)
      .values(transfer)
      .returning();
    return inserted;
  }

  async createAdminUser(admin: InsertAdminUser): Promise<AdminUser> {
    const [created] = await db
      .insert(schema.adminUsers)
      .values(admin)
      .returning();
    return created;
  }

  async getAdminByEmail(email: string): Promise<AdminUser | undefined> {
    const [admin] = await db
      .select()
      .from(schema.adminUsers)
      .where(eq(schema.adminUsers.email, email));
    return admin;
  }

  async getStats(): Promise<{
    latestBlock: number;
    totalTransactions: number;
    totalAddresses: number;
    totalContracts: number;
    isConnected: boolean;
  }> {
    const latestBlock = await this.getLatestBlock();
    const syncState = await this.getSyncState();

    const [txCount] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.transactions);

    const totalAddresses = await this.getTotalAddresses();
    const totalContracts = await this.getTotalVerifiedContracts();

    return {
      latestBlock,
      totalTransactions: txCount?.count || 0,
      totalAddresses,
      totalContracts,
      isConnected: syncState?.isConnected || false,
    };
  }

  async getAdminStats(): Promise<{
    pendingLogos: number;
    approvedLogos: number;
    rejectedLogos: number;
    totalBlocks: number;
    totalTransactions: number;
  }> {
    const [pending] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.tokens)
      .where(eq(schema.tokens.logoStatus, "pending"));

    const [approved] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.tokens)
      .where(eq(schema.tokens.logoStatus, "approved"));

    const [rejected] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.tokens)
      .where(eq(schema.tokens.logoStatus, "rejected"));

    const [blocks] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.blocks);

    const [txs] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.transactions);

    return {
      pendingLogos: pending?.count || 0,
      approvedLogos: approved?.count || 0,
      rejectedLogos: rejected?.count || 0,
      totalBlocks: blocks?.count || 0,
      totalTransactions: txs?.count || 0,
    };
  }

  async getDailyTransactionStats(days: number): Promise<Array<{
    date: string;
    count: number;
    totalValue: string;
  }>> {
    const startTime = Math.floor(Date.now() / 1000) - (days * 24 * 60 * 60);

    const transactions = await db
      .select({
        timestamp: schema.transactions.timestamp,
        value: schema.transactions.value,
      })
      .from(schema.transactions)
      .where(sql`${schema.transactions.timestamp} >= ${startTime}`)
      .orderBy(schema.transactions.timestamp);

    const dailyStats = new Map<string, { count: number; totalValue: bigint }>();

    transactions.forEach((tx: { timestamp: number; value: string }) => {
      const date = new Date(Number(tx.timestamp) * 1000).toISOString().split('T')[0];
      const existing = dailyStats.get(date) || { count: 0, totalValue: BigInt(0) };
      existing.count += 1;
      existing.totalValue += BigInt(tx.value || 0);
      dailyStats.set(date, existing);
    });

    const now = new Date();
    const result = [];
    for (let i = days - 1; i >= 0; i--) {
      const date = new Date(now.getTime() - i * 24 * 60 * 60 * 1000);
      const dateStr = date.toISOString().split('T')[0];
      const stats = dailyStats.get(dateStr) || { count: 0, totalValue: BigInt(0) };
      result.push({
        date: dateStr,
        count: stats.count,
        totalValue: stats.totalValue.toString(),
      });
    }

    return result;
  }

  async getTopWallets(limit: number): Promise<Array<{
    address: string;
    balance: string;
    type: string;
    transactionCount: number;
  }>> {
    // Filter out blacklisted addresses using a subquery check or left join
    // Since we can't easily do NOT IN (subquery) with simple drizzle selection without sql,
    // we'll fetch blacklisted first or use a left join where null.
    // Using left join technique:

    return db
      .select({
        address: schema.addresses.address,
        balance: schema.addresses.balance,
        type: schema.addresses.type,
        transactionCount: schema.addresses.transactionCount,
      })
      .from(schema.addresses)
      .leftJoin(schema.blacklistedAddresses, eq(sql`lower(${schema.addresses.address})`, schema.blacklistedAddresses.address))
      .where(sql`${schema.blacklistedAddresses.address} IS NULL`)
      .orderBy(desc(sql`CAST(${schema.addresses.balance} AS NUMERIC)`))
      .limit(limit);
  }

  private async deleteAddressData(address: string): Promise<void> {
    const normalizedAddress = address.toLowerCase();

    // 1. Delete token transfers involved (FK to transactions and tokens)
    await db.delete(schema.tokenTransfers).where(
      or(
        sql`lower(${schema.tokenTransfers.fromAddress}) = ${normalizedAddress}`,
        sql`lower(${schema.tokenTransfers.toAddress}) = ${normalizedAddress}`
      )
    );

    // 2. Delete internal transactions involved (FK to transactions)
    await db.delete(schema.internalTransactions).where(
      or(
        sql`lower(${schema.internalTransactions.fromAddress}) = ${normalizedAddress}`,
        sql`lower(${schema.internalTransactions.toAddress}) = ${normalizedAddress}`
      )
    );

    // 3. Delete transactions involved (FK to blocks, but referred by transfers/internal)
    await db.delete(schema.transactions).where(
      or(
        sql`lower(${schema.transactions.fromAddress}) = ${normalizedAddress}`,
        sql`lower(${schema.transactions.toAddress}) = ${normalizedAddress}`
      )
    );

    // 4. Delete token holders entry
    await db.delete(schema.tokenHolders).where(sql`lower(${schema.tokenHolders.holderAddress}) = ${normalizedAddress}`);

    // 5. Delete contracts (FK to addresses)
    await db.delete(schema.contracts).where(sql`lower(${schema.contracts.address}) = ${normalizedAddress}`);

    // 6. Delete tokens (FK to addresses)
    await db.delete(schema.tokens).where(sql`lower(${schema.tokens.address}) = ${normalizedAddress}`);

    // 7. Delete from addresses table (Root)
    await db.delete(schema.addresses).where(sql`lower(${schema.addresses.address}) = ${normalizedAddress}`);
  }

  async getTPS(): Promise<number> {
    const oneMinuteAgo = Math.floor(Date.now() / 1000) - 60;

    const [result] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.transactions)
      .where(sql`${schema.transactions.timestamp} >= ${oneMinuteAgo}`);

    return (result?.count || 0) / 60;
  }

  async getGasFeeStats(): Promise<{
    avgGasPrice: string;
    minGasPrice: string;
    maxGasPrice: string;
  }> {
    const recentTxs = await db
      .select({
        gasPrice: schema.transactions.gasPrice,
      })
      .from(schema.transactions)
      .orderBy(desc(schema.transactions.timestamp))
      .limit(100);

    if (recentTxs.length === 0) {
      return {
        avgGasPrice: "0",
        minGasPrice: "0",
        maxGasPrice: "0",
      };
    }

    const gasPrices = recentTxs
      .map((tx) => BigInt(tx.gasPrice || 0));

    if (gasPrices.length === 0) {
      return {
        avgGasPrice: "0",
        minGasPrice: "0",
        maxGasPrice: "0",
      };
    }

    const sum = gasPrices.reduce((a, b) => a + b, BigInt(0));
    const avg = sum / BigInt(gasPrices.length);
    const min = gasPrices.reduce((a, b) => a < b ? a : b);
    const max = gasPrices.reduce((a, b) => a > b ? a : b);

    return {
      avgGasPrice: avg.toString(),
      minGasPrice: min.toString(),
      maxGasPrice: max.toString(),
    };
  }

  async getTokenHolders(tokenAddress: string, limit: number): Promise<TokenHolder[]> {
    return db
      .select()
      .from(schema.tokenHolders)
      .where(eq(schema.tokenHolders.tokenAddress, tokenAddress))
      .orderBy(desc(sql`CAST(${schema.tokenHolders.balance} AS NUMERIC)`))
      .limit(limit);
  }

  async upsertTokenHolder(tokenAddress: string, holderAddress: string, balance: string): Promise<TokenHolder> {
    const existing = await db
      .select()
      .from(schema.tokenHolders)
      .where(
        and(
          eq(schema.tokenHolders.tokenAddress, tokenAddress),
          eq(schema.tokenHolders.holderAddress, holderAddress)
        )
      )
      .limit(1);

    if (existing.length > 0) {
      await db
        .update(schema.tokenHolders)
        .set({
          balance,
          lastUpdated: new Date(),
        })
        .where(eq(schema.tokenHolders.id, existing[0].id));

      return { ...existing[0], balance, lastUpdated: new Date() };
    }

    const [inserted] = await db
      .insert(schema.tokenHolders)
      .values({
        tokenAddress,
        holderAddress,
        balance,
        lastUpdated: new Date(),
      })
      .returning();

    return inserted;
  }

  async getHolderTokens(holderAddress: string): Promise<Array<{
    tokenAddress: string;
    balance: string;
    token: Token | null;
  }>> {
    const holdings = await db
      .select({
        tokenAddress: schema.tokenHolders.tokenAddress,
        balance: schema.tokenHolders.balance,
      })
      .from(schema.tokenHolders)
      .where(eq(schema.tokenHolders.holderAddress, holderAddress))
      .orderBy(desc(sql`CAST(${schema.tokenHolders.balance} AS NUMERIC)`));

    const result = await Promise.all(
      holdings.map(async (h: { tokenAddress: string; balance: string }) => {
        const token = await this.getToken(h.tokenAddress);
        return {
          tokenAddress: h.tokenAddress,
          balance: h.balance,
          token: token || null,
        };
      })
    );

    return result;
  }

  async getTokenHolderCount(tokenAddress: string): Promise<number> {
    const [result] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.tokenHolders)
      .where(
        and(
          eq(schema.tokenHolders.tokenAddress, tokenAddress),
          sql`CAST(${schema.tokenHolders.balance} AS NUMERIC) > 0`
        )
      );
    return result?.count || 0;
  }

  async getTokenTransferCount(tokenAddress: string): Promise<number> {
    const [result] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.tokenTransfers)
      .where(eq(schema.tokenTransfers.tokenAddress, tokenAddress));
    return result?.count || 0;
  }

  async hasTokenTransfers(txHash: string): Promise<boolean> {
    const [result] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.tokenTransfers)
      .where(eq(schema.tokenTransfers.txHash, txHash));
    return (result?.count || 0) > 0;
  }

  async getTokenTransfers(tokenAddress: string, limit: number, offset: number): Promise<TokenTransfer[]> {
    return db
      .select()
      .from(schema.tokenTransfers)
      .where(eq(schema.tokenTransfers.tokenAddress, tokenAddress))
      .orderBy(desc(schema.tokenTransfers.timestamp))
      .limit(limit)
      .offset(offset);
  }

  async getTokenHoldersPaginated(tokenAddress: string, limit: number, offset: number): Promise<TokenHolder[]> {
    return db
      .select()
      .from(schema.tokenHolders)
      .where(
        and(
          eq(schema.tokenHolders.tokenAddress, tokenAddress),
          sql`CAST(${schema.tokenHolders.balance} AS NUMERIC) > 0`
        )
      )
      .orderBy(desc(sql`CAST(${schema.tokenHolders.balance} AS NUMERIC)`))
      .limit(limit)
      .offset(offset);
  }

  async getAddressTokens(address: string): Promise<any[]> {
    return db
      .select({
        tokenAddress: schema.tokenHolders.tokenAddress,
        balance: schema.tokenHolders.balance,
        name: schema.tokens.name,
        symbol: schema.tokens.symbol,
        decimals: schema.tokens.decimals,
        logoUrl: schema.tokens.logoUrl,
        logoStatus: schema.tokens.logoStatus
      })
      .from(schema.tokenHolders)
      .leftJoin(schema.tokens, eq(schema.tokenHolders.tokenAddress, schema.tokens.address))
      .where(
        and(
          eq(schema.tokenHolders.holderAddress, address),
          sql`CAST(${schema.tokenHolders.balance} AS NUMERIC) > 0`
        )
      );
  }

  async getTokenTransfersByTx(txHash: string): Promise<any[]> {
    return db
      .select({
        tokenAddress: schema.tokenTransfers.tokenAddress,
        fromAddress: schema.tokenTransfers.fromAddress,
        toAddress: schema.tokenTransfers.toAddress,
        value: schema.tokenTransfers.value,
        name: schema.tokens.name,
        symbol: schema.tokens.symbol,
        decimals: schema.tokens.decimals,
        logoUrl: schema.tokens.logoUrl
      })
      .from(schema.tokenTransfers)
      .leftJoin(schema.tokens, eq(schema.tokenTransfers.tokenAddress, schema.tokens.address))
      .where(eq(schema.tokenTransfers.txHash, txHash));
  }

  async blacklistAddress(address: string, reason?: string): Promise<void> {
    const normalizedAddress = address.toLowerCase();
    // 1. Add to blacklist table
    await db
      .insert(schema.blacklistedAddresses)
      .values({ address: normalizedAddress, reason })
      .onConflictDoNothing();

    // 2. Delete all related data
    await this.deleteAddressData(normalizedAddress);

    // 3. Clear relevant caches
    await import("./cache").then(({ cache }) => {
      cache.del("top-wallets:10");
      cache.del("top-wallets:20");
      cache.del("top-wallets:50");
      cache.del("top-wallets:100");
    });
  }

  async unblacklistAddress(address: string): Promise<void> {
    await db
      .delete(schema.blacklistedAddresses)
      .where(eq(schema.blacklistedAddresses.address, address.toLowerCase()));

    // Clear relevant caches
    await import("./cache").then(({ cache }) => {
      cache.del("top-wallets:10");
      cache.del("top-wallets:20");
      cache.del("top-wallets:50");
      cache.del("top-wallets:100");
    });
  }

  async isAddressBlacklisted(address: string): Promise<boolean> {
    const [result] = await db
      .select({ count: sql<number>`count(*)` })
      .from(schema.blacklistedAddresses)
      .where(eq(schema.blacklistedAddresses.address, address.toLowerCase()));
    return (result?.count || 0) > 0;
  }

  async getBlacklistedAddresses(): Promise<BlacklistedAddress[]> {
    return db
      .select()
      .from(schema.blacklistedAddresses)
      .orderBy(desc(schema.blacklistedAddresses.addedAt));
  }


}

export const storage = new DatabaseStorage();
