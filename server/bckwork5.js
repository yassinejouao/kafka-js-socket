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
  sessionTimeout: 0,
  maxWaitTimeInMs: 0,
  maxBytes: 1000000,
});
const consumerMap = new Map();

const runc = async (consumer, topic) => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: topic, fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const timestamp = new Date().toISOString();
        console.log(
          `Received message for user ${topic} from topic ${topic}: ${message.value.toString()} :::::: ${timestamp}`
        );
        io.to(topic.toString()).emit("message", {
          value: message.value.toString(),
          topic: topic.toString(),
          partition: partition.toString(),
        });
      },
    });
  } catch (error) {
    console.error(`Error in runc function: ${error}`);
    await consumer.disconnect();
    setTimeout(() => runc(consumer, topic), 1000);
  }
};

io.on("connection", (socket) => {
  let user = "";
  let consumer = "";
  console.log("new user connected");
  socket.on("conn", async (data) => {
    user = data.userId;
    socket.join(data.userId);
    consumer = kafka.consumer({ groupId: "gps" });
    consumerMap.set(data.userId, consumer);
    runc(consumer, data.userId);
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
