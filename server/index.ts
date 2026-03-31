import express, { type Request, Response, NextFunction } from "express";
import { registerRoutes } from "./routes";
import { setupVite, serveStatic, log } from "./vite";
import dotenv from "dotenv";
import os from "os";

dotenv.config();
const app = express();

declare module "http" {
  interface IncomingMessage {
    rawBody: unknown;
  }
}

app.use(
  express.json({
    verify: (req, _res, buf) => {
      req.rawBody = buf;
    },
  })
);

app.use(express.urlencoded({ extended: false }));

app.use((req, res, next) => {
  const start = Date.now();
  const path = req.path;
  let capturedJsonResponse: Record<string, any> | undefined = undefined;

  const originalResJson = res.json;

  res.json = function (bodyJson, ...args) {
    capturedJsonResponse = bodyJson;
    return originalResJson.apply(res, [bodyJson, ...args]);
  };

  res.on("finish", () => {
    const duration = Date.now() - start;

    if (path.startsWith("/api")) {
      let logLine = `${req.method} ${path} ${res.statusCode} in ${duration}ms`;

      if (capturedJsonResponse) {
        logLine += ` :: ${JSON.stringify(capturedJsonResponse)}`;
      }

      if (logLine.length > 80) {
        logLine = logLine.slice(0, 79) + "…";
      }

      log(logLine);
    }
  });

  next();
});

(async () => {
  const server = await registerRoutes(app);

  app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
    const status = err.status || err.statusCode || 500;
    const message = err.message || "Internal Server Error";

    res.status(status).json({ message });
    throw err;
  });

  if (app.get("env") === "development") {
    await setupVite(app, server);
  } else {
    serveStatic(app);
  }

  console.log("🔍 ENV DEBUG:", {
    RPC_URL: process.env.RPC_URL,
    WSS_URL: process.env.WSS_URL,
  });

  const port = parseInt(process.env.PORT || "5000", 10);

  server.listen(
    {
      port,
      host: "0.0.0.0",
    },
    () => {
      const interfaces = os.networkInterfaces();
      let networkIp = "localhost";

      for (const name of Object.keys(interfaces)) {
        const ifaceList = interfaces[name];
        if (!ifaceList) continue;

        for (const iface of ifaceList) {
          if (iface.family === "IPv4" && !iface.internal) {
            networkIp = iface.address;
          }
        }
      }

      console.log("\n🚀 Server started\n");
      console.log(`Local:    http://localhost:${port}`);
      console.log(`Network:  http://${networkIp}:${port}`);
      console.log(`Env:      ${app.get("env")}\n`);

      log(`serving on port ${port}`);
    }
  );
})();