FROM node:18.18.2
# Create app directory
WORKDIR /usr/src/app
# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
COPY package*.json ./
RUN npm install
# If you are building your code for production
# RUN npm ci --only=production

# Bundle app source
# docker run -p 3000:3000 -d docker_demo//TO run Docker file
# docker logs -f 1c469512bba9 //for logs
COPY . .
EXPOSE 3002
CMD [ "npm", "run", "start" ]