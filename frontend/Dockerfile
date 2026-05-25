FROM node:20-alpine

WORKDIR /app

# Install dependencies
COPY package.json package-lock.json* ./
RUN npm install

# Copy application code
COPY . .

# Expose port
EXPOSE 3000

# Start Vite dev server (host 0.0.0.0 is required for Docker)
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "3000"]
