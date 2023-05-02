const express = require("express");
require("dotenv").config();
const cors = require("cors");
const S3Watcher = require("./s3-watch");

const app = express();
// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());

const s3Watcher = new S3Watcher();

// Start watching the S3 bucket for changes
s3Watcher.watch(
  (newObjects) => {
    console.log("newObjects", newObjects);
    console.log(`Detected ${newObjects.length} new objects in the bucket`);
    // Trigger your job here using the newObjects array
    // Download and save each new object
    processNewObjects(newObjects);
  },
  "TG-46350/",
  ".xml"
);

// Route
app.get("/", async (req, res) => {
  res.send("Hello World!");
});

// Start the Express server
app.listen(3000, () => {
  console.log("Server started on port 3000");
});
