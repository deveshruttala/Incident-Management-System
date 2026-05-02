FROM node:20-alpine AS build
WORKDIR /app
COPY package.json ./
RUN npm install --silent
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
# alpine ships with wget via busybox but the option set varies; use a TCP probe
# via /dev/tcp, which nginx:alpine's BusyBox shell supports universally.
HEALTHCHECK --interval=15s --timeout=3s \
    CMD nginx -t -q && wget --spider -q http://127.0.0.1/ || exit 1
