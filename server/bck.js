// const express = require("express");
// const app = express();

// const PORT = process.env.PORT || 3000;

// // Define route
// app.get("/route", (req, res) => {
//   const delay = parseInt(req.query.delay) || 0;

//   // Simulate an asynchronous operation using the specified delay or 0 if not provided
//   setTimeout(() => {
//     res.send(`Response with a delay of ${delay}ms`);
//   }, delay);
// });

// // Start server
// app.listen(PORT, () => {
//   console.log(`Server started on port ${PORT}`);
// });

const express = require("express");
const app = express();
const cors = require("cors");
const server = require("http").createServer(app);
const io = require("socket.io")(server, {
  cors: {
    origin: "*",
  },
});

const kafka = require("kafka-node");

const users = new Map();
const topics = [""];
const consumerGroup = new kafka.ConsumerGroup(
  {
    kafkaHost: "localhost:9092",
    groupId: "gps",
    autoCommit: true,
    fromOffset: "latest",
  },
  [""]
);

io.on("connection", (socket) => {
  socket.on("conn", (data) => {
    console.log("new user connected");
    users.set(socket.id, data.userId);
    socket.join(data.userId);
    if (io.of("/").adapter.rooms.has(data.userId)) {
      console.log("my-room exists");
    } else {
      console.log("my-room does not exist");
    }
    if (!topics.includes(data.userId)) {
      consumerGroup.addTopics([data.userId], (error, added) => {
        if (error) {
          console.error(`Failed to add topic ${data.userId}:`, error);
        } else {
          console.log(`Added topic ${data.userId}`);
          console.log("TOPICS2 :::::");
          console.log([...topics.entries()]);
        }
      });
    }
    console.log("USERS :::::");
    console.log([...users.entries()]);
    console.log("TOPICS :::::");
    console.log([...topics.entries()]);
  });
  socket.on("disconnect", () => {
    let usertodelete = users.get(socket.id);

    users.delete(socket.id);
    if (topics.includes(usertodelete)) {
      consumerGroup.removeTopics([usertodelete], (err, removed) => {
        if (err) {
          console.error(`Failed to remove topics: ${err}`);
          return;
        }
        console.log(`Successfully removed topics: ${removed}`);
      });
    }
    console.log([...users.entries()]);
  });
});

consumerGroup.on("message", (message) => {
  console.log(`message from topic ${message.topic}: ${message.value}`);
  io.to(message.topic).emit("message", message.value);
});

server.listen(3000, function () {
  console.log("Server listening on port 3000");
});
