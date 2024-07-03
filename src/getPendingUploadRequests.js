require("dotenv").config();
const retryTransfer = require("./retryTransfer.js");
const queryTypes = require("../util/queryTypes");
const queryDB = queryTypes.queryDB();

const blockchain_array = JSON.parse(process.env.SUPPORTED_BLOCKCHAINS);
const wallet_array = JSON.parse(process.env.WALLET_ARRAY);

module.exports = {
  getPendingUploadRequests: async function getPendingUploadRequests() {
    try {
      console.log(`Checking for transactions to process...`);

      let pending_requests = [];
      for (const blockchain of blockchain_array) {
        query =
          "select th.txn_id,th.progress,th.approver,th.blockchain,dh.asset_data,th.keywords,th.epochs,th.updated_at,th.created_at,th.receiver FROM txn_header th join data_header dh on dh.data_id = th.data_id where th.progress = ? and th.blockchain = ? and th.request = 'Create-n-Transfer' ORDER BY th.created_at ASC LIMIT 1";
        params = ["PENDING", blockchain.name];
        let request = await queryDB
          .getData(query, params)
          .then((results) => {
            //console.log('Query results:', results);
            return results;
            // Use the results in your variable or perform further operations
          })
          .catch((error) => {
            console.error("Error retrieving data:", error);
          });

        let available_wallets = [];
        for (const wallet of wallet_array) {
          query = `select th.txn_id,th.progress,th.approver,th.blockchain,dh.asset_data,th.keywords,th.epochs,th.updated_at,th.created_at,th.receiver,th.ual,th.paranet_ual from txn_header th join data_header dh on dh.data_id = th.data_id where th.request = 'Create-n-Transfer' AND th.approver = ? AND th.blockchain = ? order by th.updated_at desc LIMIT 5`;
          params = [wallet.public_key, blockchain.name];
          last_processed = await queryDB
            .getData(query, params)
            .then((results) => {
              //console.log('Query results:', results);
              return results;
              // Use the results in your variable or perform further operations
            })
            .catch((error) => {
              console.error("Error retrieving data:", error);
            });

          if (Number(last_processed.length) === 0) {
            available_wallets.push(wallet);
            continue;
          }

          let updatedAtTimestamp = last_processed[0].updated_at;
          let currentTimestamp = new Date();
          let timeDifference = currentTimestamp - updatedAtTimestamp;

          //create nhung up and never happened
          if (
            last_processed[0].progress === "PROCESSING" &&
            timeDifference >= 600000
          ) {
            console.log(
              `${wallet.name} wallet ${wallet.public_key}: Processing for over 10 minutes. Rolling back to pending...`
            );
            query = `UPDATE txn_header SET progress = ?, approver = ? WHERE approver = ? AND progress = ? AND request = 'Create-n-Transfer' and blockchain = ?`;
            params = ["PENDING", null, wallet.public_key, "PROCESSING", blockchain.name];
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

            available_wallets.push(wallet);
            continue;
          }

          //3 records of retrying a transfer (index 0 is 4th failed-transfer txn)
          if (last_processed[4]) {
            if (last_processed[4].progress === "RETRYING-TRANSFER") {
              console.log(
                `${wallet.name} ${wallet.public_key}: Transfer attempt failed 3 times. Abandoning transfer...`
              );
              query = `UPDATE txn_header SET progress = ? WHERE progress in (?,?) AND approver = ? and blockchain = ?`;
              params = [
                "TRANSFER-ABANDONED",
                "CREATED",
                "RETRYING-TRANSFER",
                wallet.public_key,
                blockchain.name
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

              available_wallets.push(wallet);
              continue;
            }
          }

          //last transfer attempt failed and there were 3 retry failures yet
          if (last_processed[0].progress === "TRANSFER-FAILED") {
            console.log(
              `${wallet.name} wallet ${wallet.public_key}: Retrying failed transfer...`
            );

            await retryTransfer.retryTransfer(last_processed[0]);
            continue;
          }

          //not processing, not transfering, not retrying transfer
          if (
            last_processed[0].progress !== "PROCESSING" &&
            last_processed[0].progress !== "CREATED" &&
            last_processed[0].progress !== "TRANSFER-FAILED"
          ) {
            available_wallets.push(wallet);
          }
        }

        console.log(
          `${blockchain.name} has ${available_wallets.length} available wallets.`
        );

        if (Number(available_wallets.length) === 0) {
          continue;
        }

        if (Number(request.length) === 0) {
          console.log(`${blockchain.name} has no pending requests.`);
        } else {
          request[0].approver = available_wallets[0].public_key;
          pending_requests.push(request[0]);
        }
      }

      return pending_requests;
    } catch (error) {
      throw new Error("Error fetching pending requests: " + error.message);
    }
  },
};
