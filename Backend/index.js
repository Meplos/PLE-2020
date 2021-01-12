const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");

const PORT = 3000;

const app = express();

app.use(bodyParser.urlencoded());
app.use(bodyParser.json());
app.use(cors());

app.get("/", (req, res) => res.status(400).send("Welcome 👌"));

app.listen(PORT, () => console.log("Server Listening"));
