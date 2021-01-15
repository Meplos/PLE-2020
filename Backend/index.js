const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const hbase = require("hbase");

const PORT = 6783;
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
      result["words"] = [];
      rows.forEach((element) => {
        if (!result[element.key]) {
          result[element.key] = [];
        }
        if (!result["words"].includes(element.key)) {
          result["words"].push(element.key);
        }
        const obj = {};
        obj.date = element.column.split(":")[1];
        obj.total = element.$;
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

app.get("/language_topk/", (req, res) => {
  const rows = [];

  client.table("gresse_langage_topk").scan({}, (err, chunks) => {
    if (err) {
      console.log("ERR " + err);
      res.send(err.status);
    } else {
      chunks.forEach((element) => {
        let isAlreadyDefined = false;
        let obj = {};
        console.log(chunks.find((x) => x.key === element.key));
        if (!rows.find((x) => x.key === element.key)) {
          obj.key = element.key;
        } else {
          isAlreadyDefined = true;
          obj = rows.find((x) => x.key === element.key);
        }
        let col = element.column.split(":")[1];
        if (col === "count") {
          obj.count = element.$;
        } else {
          obj.lang = element.$;
        }
        if (!isAlreadyDefined) rows.push(obj);
      });
    }
    rows.sort(compare);
    res.status(200).send(rows);
  });
});

app.get("/location_topk/:loc", (req, res) => {
  const loc = req.params.loc;
  const rows = [];
  client
    .table("gresse_location_topk")
    .row(loc)
    .get("", (err, chunks) => {
      if (err) {
        console.log("ERR " + err);
        res.send(err.status);
      } else {
        console.log("Size : " + chunks.length);
        chunks.forEach((element) => {
          let isAlreadyDefined = false;
          let obj = {};
          let key = element.column.split(":")[0];
          let col = element.column.split(":")[1];
          console.log(chunks.find((x) => x.key === key));
          if (!rows.find((x) => x.key === key)) {
            obj.key = key;
          } else {
            isAlreadyDefined = true;
            obj = rows.find((x) => x.key === key);
          }
          if (col === "total") {
            obj.count = element.$;
          } else {
            obj.name = element.$;
          }
          if (!isAlreadyDefined) rows.push(obj);
        });
        console.log(chunks);
        rows.sort(compare);
        res.status(200).send(rows);
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

let compare = (x, y) => {
  if (eval(x.key) > eval(y.key)) return 1;
  else if (eval(x.key) < eval(y.key)) return -1;
  else return 0;
};

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
