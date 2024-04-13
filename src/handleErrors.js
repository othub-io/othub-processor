require("dotenv").config();
const queryTypes = require("../util/queryTypes");
const queryDB = queryTypes.queryDB();

const wallet_array = JSON.parse(process.env.WALLET_ARRAY);

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

module.exports = {
  handleError: async function handleError(message) {
    try {
      let query;
      let params;
      console.log(message)
      if (
        message.error === "Safe mode validation error." ||
        message.error === "File format is corrupted, no n-quads are extracted." ||
        message.error.includes("undefined")
      ) {
        console.log(
          `${wallet_array[message.index].name} wallet ${
            wallet_array[message.index].public_key
          }: Create failed. ${message.error} Abandoning...`
        );
        query = `UPDATE txn_header SET progress = ?, data_id = ? WHERE approver = ? AND request = 'Create-n-Transfer' AND progress = ? AND blockchain = ?`;
        params = [
          "CREATE-ABANDONED",
          null,
          wallet_array[message.index].public_key,
          "PROCESSING",
          message.blockchain
        ];
        await queryDB
          .getData(query, params)
          .then((results) => {
            //console.log('Query results:', results);
            return results;
            // Use the results in your variable or perform further operations
          })
          .catch((error) => {
            console.error("Error retrieving data:", error);
          });
        return;
      }

      if (message.request === "Create-n-Transfer") {
        console.log(
          `${wallet_array[message.index].name} wallet ${
            wallet_array[message.index].public_key
          }: Create failed. ${
            message.error
          }. Reverting to pending in 1 minute...`
        );
        await sleep(60000);

        query = `UPDATE txn_header SET progress = ?, approver = ? WHERE approver = ? AND request = 'Create-n-Transfer' AND progress = ? AND blockchain = ?`;
        params = [
          "PENDING",
          null,
          wallet_array[message.index].public_key,
          "PROCESSING",
          message.blockchain
        ];
        await queryDB
          .getData(query, params)
          .then((results) => {
            //console.log('Query results:', results);
            return results;
            // Use the results in your variable or perform further operations
          })
          .catch((error) => {
            console.error("Error retrieving data:", error);
          });
        return;
      }

      if (message.request === "Transfer") {
        console.log(
          `${wallet_array[message.index].name} wallet ${
            wallet_array[message.index].public_key
          }: Transfer failed. ${message.error}. Retrying in 1 minute...`
        );
        await sleep(60000);

        query = `INSERT INTO txn_header (txn_id, progress, approver, key_id, request, blockchain, app_name, txn_description, data_id, ual, keywords, state, txn_hash, txn_fee, trac_fee, epochs, receiver) VALUES (UUID(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`;
        params = [
          "TRANSFER-FAILED",
          wallet_array[message.index].public_key,
          null,
          "Create-n-Transfer",
          message.blockchain,
          null,
          null,
          null,
          message.ual,
          null,
          null,
          null,
          null,
          null,
          null,
          message.receiver,
        ];
        await queryDB
          .getData(query, params)
          .then((results) => {
            //console.log('Query results:', results);
            return results;
            // Use the results in your variable or perform further operations
          })
          .catch((error) => {
            console.error("Error retrieving data:", error);
          });
        return;
      }

      console.log(
        `${wallet_array[message.index].name} wallet ${
          wallet_array[message.index].public_key
        }: Unexpected Error. ${message.error}. Abandoning...`
      );
      query = `UPDATE txn_header SET progress = ?, data_id = ? WHERE approver = ? AND request = 'Create-n-Transfer' AND progress = ? AND blockchain = ?`;
      params = [
        "CREATE-ABANDONED",
        null,
        wallet_array[message.index].public_key,
        "PROCESSING",
        message.blockchain
      ];
      await queryDB
        .getData(query, params)
        .then((results) => {
          //console.log('Query results:', results);
          return results;
          // Use the results in your variable or perform further operations
        })
        .catch((error) => {
          console.error("Error retrieving data:", error);
        });
      return;
    } catch (error) {
      console.log(error);
    }
  },
};
