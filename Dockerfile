FROM node:24.22.1
# Create app directory
WORKDIR /usr/src/app
# Run as the unprivileged node user, which also owns the app folder
RUN chown node:node /usr/src/app
USER node
# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
COPY --chown=node:node package*.json ./
RUN npm ci
# If you are building your code for production

# Bundle app source
COPY --chown=node:node . .
CMD [ "npm", "run", "start" ]