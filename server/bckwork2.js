const express = require("express");

const app = express();
const server = require("http").createServer(app);
const io = require("socket.io")(server, {
  cors: {
    origin: "*",
  },
});
const { Kafka } = require("kafkajs");
const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "gps" });
async function consumerConnect() {
  await consumer.connect();
  await consumer.subscribe({ topic: "messages" });
  console.log("fisrt subscribe::");
}
consumerConnect();
async function run() {
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
        topic: topic.toString(),
        partition: partition.toString(),
      });
      io.to(topic.toString()).emit("message", {
        value: message.value.toString(),
        topic: topic.toString(),
        partition: partition.toString(),
      });
    },
  });
}
async function subscribeTopic(topic) {
  await consumer.stop();
  await consumer.subscribe({ topic: topic, fromBeginning: true });
  console.log("subscribe::", topic);
  run();
}
// async function unsubscribeTopic(topic) {
//   await consumer.stop();
//   await consumer.unsubscribe(topic);
//   await consumer.run();
// }

// function findKeyByValue(map, value) {
//   for (const [key, val] of map.entries()) {
//     if (val === value) {
//       return key;
//     }
//   }
//   return null;
// }

// function findKeysByValue(map, value) {
//   return Array.from(map.entries())
//     .filter((entry) => entry[1] === value)
//     .map((entry) => entry[0]);
// }

// const topics = new Map();
// const users = new Map();

// const consumerMap = new Map();

io.on("connection", (socket) => {
  console.log("new user connected");
  socket.on("conn", async (data) => {
    socket.join(data.userId);
    await subscribeTopic(data.userId);
  });

  // socket.on("disconnect", () => {
  //   let usertodelete = users.get(socket.id);
  //   users.delete(socket.id);
  //   if (
  //     findKeysByValue(users, usertodelete).length == 0 &&
  //     usertodelete != undefined
  //   ) {
  //     unsubscribeTopic(usertodelete);
  //   }
  // });
});

// consumerGroup.on("message", (message) => {
//   console.log(`message from topic ${message.topic}: ${message.value}`);
//   console.log([...topics.entries()]);
//   io.to(message.topic).emit("message", message.value);
// });

server.listen(3000, function () {
  console.log("Server listening on port 3000");
});
