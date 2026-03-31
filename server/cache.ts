import Redis from "ioredis";

let redis: Redis | null = null;

const REDIS_URL = process.env.REDIS_URL || process.env.UPSTASH_REDIS_REST_URL;
const CACHE_ENABLED = !!REDIS_URL;

if (CACHE_ENABLED && REDIS_URL) {
  try {
    redis = new Redis(REDIS_URL);
    console.log("Redis cache connected");
    
    redis.on("error", (error) => {
      console.error("Redis error:", error);
    });
  } catch (error) {
    console.warn("Redis connection failed, running without cache:", error);
  }
} else {
  console.log("Redis not configured, running without cache");
}

const DEFAULT_TTL = 30;

interface CacheOptions {
  ttl?: number;
}

export class CacheService {
  static async get<T>(key: string): Promise<T | null> {
    if (!redis) return null;
    
    try {
      const data = await redis.get(key);
      if (!data) return null;
      
      return JSON.parse(data) as T;
    } catch (error) {
      console.error("Cache get error:", error);
      return null;
    }
  }

  static async set(key: string, value: any, options: CacheOptions = {}): Promise<void> {
    if (!redis) return;
    
    try {
      const ttl = options.ttl || DEFAULT_TTL;
      await redis.set(key, JSON.stringify(value), "EX", ttl);
    } catch (error) {
      console.error("Cache set error:", error);
    }
  }

  static async del(key: string): Promise<void> {
    if (!redis) return;
    
    try {
      await redis.del(key);
    } catch (error) {
      console.error("Cache delete error:", error);
    }
  }

  static async delPattern(pattern: string): Promise<void> {
    if (!redis) return;
    
    try {
      const keys = await redis.keys(pattern);
      if (keys.length > 0) {
        await redis.del(...keys);
      }
    } catch (error) {
      console.error("Cache delete pattern error:", error);
    }
  }

  static async invalidateBlockData(): Promise<void> {
    await Promise.all([
      this.delPattern("blocks:*"),
      this.delPattern("transactions:*"),
      this.delPattern("stats"),
      this.delPattern("latest:*"),
    ]);
  }

  static async invalidateAddress(address: string): Promise<void> {
    await Promise.all([
      this.del(`address:${address}`),
      this.del(`address:${address}:transactions`),
    ]);
  }

  static async invalidateContract(address: string): Promise<void> {
    await Promise.all([
      this.del(`contract:${address}`),
      this.delPattern("contracts:*"),
    ]);
  }

  static async invalidateToken(address: string): Promise<void> {
    await Promise.all([
      this.del(`token:${address}`),
      this.delPattern("tokens:*"),
    ]);
  }
}

export const cache = CacheService;
