const express = require("express");

const app = express();
const server = require("http").createServer(app);
const io = require("socket.io")(server, {
  cors: {
    origin: "*",
  },
});

function findKeyByValue(map, value) {
  for (const [key, val] of map.entries()) {
    if (val === value) {
      return key;
    }
  }
  return null;
}

function findKeysByValue(map, value) {
  return Array.from(map.entries())
    .filter((entry) => entry[1] === value)
    .map((entry) => entry[0]);
}

const kafka = require("kafka-node");
const topics = new Map();
const users = new Map();
const consumerGroup = new kafka.ConsumerGroup(
  {
    kafkaHost: "localhost:9092",
    groupId: "gps",
    protocol: ["roundrobin"],
    fromOffset: "latest",
    autoCommit: true,
  },
  ["messages"]
);

io.on("connection", (socket) => {
  console.log("new user connected");
  socket.on("conn", (data) => {
    socket.join(data.userId);
    users.set(socket.id, data.userId);
    if (!topics.has(data.userId)) {
      topics.set(data.userId, data.userId);
      consumerGroup.addTopics([data.userId], (error, added) => {
        if (error) {
          console.error(`Failed to add topic ${data.userId}:`, error);
        } else {
          console.log(`Added topic ${data.userId}`);
        }
      });
    } else {
      console.log("topic already created");
    }
  });

  socket.on("disconnect", () => {
    let usertodelete = users.get(socket.id);
    users.delete(socket.id);
    if (
      findKeysByValue(users, usertodelete).length == 0 &&
      usertodelete != undefined
    ) {
      consumerGroup.removeTopics([usertodelete], (err, removed) => {
        if (err) {
          console.error(`Failed to remove topics: ${err}`);
        }
        topics.delete(usertodelete);
        console.log(`Successfully removed topics: ${removed}`);
      });
    }
  });
});

consumerGroup.on("message", (message) => {
  console.log(`message from topic ${message.topic}: ${message.value}`);
  console.log([...topics.entries()]);
  io.to(message.topic).emit("message", message.value);
});

server.listen(3000, function () {
  console.log("Server listening on port 3000");
});
