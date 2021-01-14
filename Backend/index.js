const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const hbase = require("hbase");

const PORT = 7000;
const client = hbase({ host: "127.0.0.1", port: 8080 });

const app = express();

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.use(cors());

app.get("/word_pop", (req, res) => {
  let result;
  client.table("gresse_word_pop").scan(
    {
      batch: 100,
    },
    function (err, rows) {
      result = {};
      if (err) {
        console.error(err);
        res.status(400).send(err);
        return;
      }
      rows.forEach((element) => {
        if (!result[element.key]) {
          result[element.key] = [];
        }
        const obj = {};
        obj.date = element.column.split(":")[1];
        obj.count = element.$;
        result[element.key].push(obj);
      });
      console.log(result);
      res.status(200).send(result);
      if (err) {
        res.sendStatus(500);
      }
    }
  );
});

app.get("/hashtag_pop/:tag", (req, res) => {
  const rows = [];
  const tag = req.params.tag;
  client
    .table("gresse_hashtag_pop")
    .row(tag)
    .get("days", (err, chunks) => {
      if (err) {
        console.log("ERR " + err);
        res.send(err.status);
      } else {
        console.log("Size : " + chunks.length);
        res.status(200).send(chunks);
      }
    });
});

app.get("/language_pop/:lang", (req, res) => {
  const rows = [];
  const lang = req.params.lang;
  client
    .table("gresse_langage_evolution")
    .row(lang)
    .get("days", (err, chunks) => {
      if (err) {
        console.log("ERR " + err);
        res.send(err.status);
      } else {
        console.log("Size : " + chunks.length);
        res.status(200).send(chunks);
      }
    });
});

app.get("/", (req, res) => res.status(400).send("Welcome 👌!"));

app.listen(PORT, () => console.log("Server Listening"));

function groupArrayOfObjects(list, key) {
  return list.reduce(function (rv, x) {
    (rv[x[key]] = rv[x[key]] || []).push(x);
    return rv;
  }, {});
}

/** /word_pop result
 * 
 *  {
 *      word: [
 *          {
 *          key: 'virus',
            column: 'number:Mar 1',
            timestamp: 1610460353756,
            '$': '16' 
            }, {
              *          key: 'virus',
            column: 'number:Mar 2',
            timestamp: 1610460353756,
            '$': '163' 
   
            }
 *      ], 
 *    confinement:  ...
 * }
 * 
 * 
 */
