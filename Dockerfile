# Builds client/ into static HTML/JS/CSS, then runs server/ which serves it
# together with the shared world and multiplayer room on one port.

# ---- Build the client ----
FROM node:22-alpine AS client-build
# Without git some packages fail to install
RUN apk add git
WORKDIR /app/client
COPY client/ /app/client/
RUN corepack enable
ARG DOWNLOAD_SOUNDS=false
ARG DISABLE_SERVICE_WORKER=false
ARG CONFIG_JSON_SOURCE=REMOTE
# Optional prep (release info, slimmer install); skipped if the folder isn't a git checkout
RUN node ./scripts/dockerPrepare.mjs || echo "dockerPrepare skipped"
RUN pnpm install
RUN if [ "$DOWNLOAD_SOUNDS" = "true" ] ; then node scripts/downloadSoundsMap.mjs ; fi
RUN DISABLE_SERVICE_WORKER=$DISABLE_SERVICE_WORKER \
    CONFIG_JSON_SOURCE=$CONFIG_JSON_SOURCE \
    pnpm run build

# ---- Run the server ----
FROM node:22-alpine
RUN apk add git
WORKDIR /app
COPY server/package.json /app/server/package.json
RUN npm --prefix /app/server install --omit=dev
COPY server/server.js /app/server/server.js
COPY --from=client-build /app/client/dist /app/client/dist
COPY client/config.json /app/client/config.json
ENV NODE_ENV=production
ENV PORT=8080
ENV WORLD_FOLDER=/app/world
# Address players connect to (matches GAME_SERVER_ORIGIN in client/src/gameServer.ts)
ENV PUBLIC_GAME_URL=wss://trueelement.shop
ENV SERVER_NAME="True Element"
EXPOSE 8080
# Mount a persistent volume here so the world survives restarts / redeploys
VOLUME /app/world
# Optional: drop files here to override client assets (e.g. a custom config.json)
VOLUME /app/client/public
CMD ["node", "/app/server/server.js", "--prod"]
