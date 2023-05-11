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
const consumerMap = new Map();

io.on("connection", (socket) => {
  let user = "";
  let consumer = "";
  console.log("new user connected");
  socket.on("conn", (data) => {
    user = data.userId;
    socket.join(data.userId);
    consumer = kafka.consumer({ groupId: data.userId, fromBeginning: false });
    consumer.subscribe({ topic: data.userId });
    consumerMap.set(data.userId, consumer);
    consumer.connect().then(() => {
      console.log(`Consumer for user ${data.userId} connected`);
      consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          console.log(
            `Received message for user ${
              data.userId
            } from topic ${topic}: ${message.value.toString()}`
          );
          io.to(topic.toString()).emit("message", {
            value: message.value.toString(),
            topic: topic.toString(),
            partition: partition.toString(),
          });
        },
      });
    });
  });

  socket.on("disconnect", () => {
    if (consumerMap.size > 0 && user.length > 0) {
      console.log(`Client ${user} disconnected`);
      consumer.disconnect().then(() => {
        console.log(`Consumer for user ${user} disconnected`);
        consumerMap.delete(user);
      });
    }
  });
});
server.listen(3000, function () {
  console.log("Server listening on port 3000");
});
