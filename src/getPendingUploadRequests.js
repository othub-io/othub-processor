require("dotenv").config();
const retryTransfer = require("./retryTransfer.js");
const queryTypes = require("../util/queryTypes");
const queryDB = queryTypes.queryDB();

const network_array = JSON.parse(process.env.SUPPORTED_NETWORKS);
const wallet_array = JSON.parse(process.env.WALLET_ARRAY);

module.exports = {
  getPendingUploadRequests: async function getPendingUploadRequests() {
    try {
      console.log(`Checking for transactions to process...`);

      let pending_requests = [];
      for (const blockchain of network_array) {
        query =
          "select txn_id,progress,approver,network,txn_data,keywords,epochs,updated_at,created_at,receiver,api_key FROM txn_header where progress = ? and network = ? and request = 'Create-n-Transfer' ORDER BY created_at ASC LIMIT 1";
        params = ["PENDING", blockchain.network];
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
          query = `select txn_id,progress,approver,network,txn_data,keywords,epochs,updated_at,created_at,receiver,ual from txn_header where request = 'Create-n-Transfer' AND approver = ? AND network = ? order by updated_at desc LIMIT 5`;
          params = [wallet.public_key, blockchain.network];
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
            query = `UPDATE txn_header SET progress = ?, approver = ? WHERE approver = ? AND progress = ? AND request = 'Create-n-Transfer' and network = ?`;
            params = ["PENDING", null, wallet.public_key, "PROCESSING", blockchain.network];
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
              query = `UPDATE txn_header SET progress = ? WHERE progress in (?,?) AND approver = ? and network = ?`;
              params = [
                "TRANSFER-ABANDONED",
                "CREATED",
                "RETRYING-TRANSFER",
                wallet.public_key,
                blockchain.network
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
          `${blockchain.network} has ${available_wallets.length} available wallets.`
        );

        if (Number(available_wallets.length) === 0) {
          continue;
        }

        if (Number(request.length) === 0) {
          console.log(`${blockchain.network} has no pending requests.`);
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
