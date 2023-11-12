require("dotenv").config();
const mysql = require("mysql");
const retryTransfer = require("./retryTransfer.js");

const othubdb_connection = mysql.createConnection({
  host: process.env.DBHOST,
  user: process.env.DBUSER,
  password: process.env.DBPASSWORD,
  database: process.env.OTHUB_DB,
});

const network_array = JSON.parse(process.env.SUPPORTED_NETWORKS);
const wallet_array = JSON.parse(process.env.WALLET_ARRAY);

function executeOTHubQuery(query, params) {
  return new Promise((resolve, reject) => {
    othubdb_connection.query(query, params, (error, results) => {
      if (error) {
        reject(error);
      } else {
        resolve(results);
      }
    });
  });
}

async function getOTHubData(query, params) {
  try {
    const results = await executeOTHubQuery(query, params);
    return results;
  } catch (error) {
    console.error("Error executing query:", error);
    throw error;
  }
}

module.exports = {
  getPendingUploadRequests: async function getPendingUploadRequests() {
    try {
      console.log(`Checking for transactions to process...`);

      let pending_requests = [];
      for (i = 0; i < network_array.length; i++) {
        sqlQuery =
          "select txn_id,progress,approver,network,txn_data,keywords,epochs,updated_at,created_at,receiver,api_key FROM txn_header where progress = ? and network = ? and request = 'Create-n-Transfer' ORDER BY created_at ASC LIMIT 1";
        params = ["PENDING", network_array[i].network];
        let request = await getOTHubData(sqlQuery, params)
          .then((results) => {
            //console.log('Query results:', results);
            return results;
            // Use the results in your variable or perform further operations
          })
          .catch((error) => {
            console.error("Error retrieving data:", error);
          });

        if (Number(request.length) === 0) {
          console.log(`${network_array[i].network} has no pending requests.`);
          continue;
        }

        let available_wallets = [];
        for (x = 0; x < wallet_array.length; x++) {
          query = `select txn_id,progress,approver,network,txn_data,keywords,epochs,updated_at,created_at,receiver,ual from txn_header where request = 'Create-n-Transfer' AND approver = ? AND network = ? order by updated_at desc LIMIT 1`;
          params = [wallet_array[x].public_key, network_array[i].network];
          last_processed = await getOTHubData(query, params)
            .then((results) => {
              //console.log('Query results:', results);
              return results;
              // Use the results in your variable or perform further operations
            })
            .catch((error) => {
              console.error("Error retrieving data:", error);
            });

          if (Number(last_processed.length) === 0) {
            available_wallets.push(wallet_array[x]);
            continue;
          }

          let updatedAtTimestamp = last_processed[0].updated_at;
          let currentTimestamp = new Date();
          let timeDifference = currentTimestamp - updatedAtTimestamp;

          if (
            last_processed[0].progress === "PROCESSING" &&
            timeDifference >= 600000
          ) {
            console.log(
              `${wallet_array[x].name} ${wallet_array[x].public_key}: Processing for over 10 minutes. Rolling back to pending...`
            );
            query = `UPDATE txn_header SET progress = ?, approver = ? WHERE approver = ? AND progress = ? AND request = 'Create-n-Transfer'`;
            params = [
              "PENDING",
              null,
              wallet_array[x].public_key,
              "PROCESSING",
            ];
            await getOTHubData(query, params)
              .then((results) => {
                //console.log('Query results:', results);
                return results;
                // Use the results in your variable or perform further operations
              })
              .catch((error) => {
                console.error("Error retrieving data:", error);
              });

            available_wallets.push(wallet_array[x]);
            continue;
          }

          if (
            last_processed[0].progress === "TRANSFER-FAILED" &&
            timeDifference >= 180000
          ) {
            query = `select count(*) AS count from txn_header where request = 'Create-n-Transfer' AND approver = ? AND network = ? AND progress = ? order by updated_at desc LIMIT 5`;
            params = [
              wallet_array[x].public_key,
              network_array[i].network,
              "TRANSFER-FAILED",
            ];
            retries = await getOTHubData(query, params)
              .then((results) => {
                //console.log('Query results:', results);
                return results;
                // Use the results in your variable or perform further operations
              })
              .catch((error) => {
                console.error("Error retrieving data:", error);
              });

              console.log(`RETRIES: `+JSON.stringify(retries))
            if (Number(retries.count) >= 5) {
              console.log(
                `${wallet_array[x].name} ${wallet_array[x].public_key}: Transfer attempt failed 5 times. Abandoning transfer...`
              );
              query = `UPDATE txn_header SET progress = ? WHERE progress = ? AND approver = ?`;
              params = [
                "ABANDONED",
                "TRANSFER-FAILED",
                wallet_array[x].public_key,
              ];
              await getOTHubData(query, params)
                .then((results) => {
                  //console.log('Query results:', results);
                  return results;
                  // Use the results in your variable or perform further operations
                })
                .catch((error) => {
                  console.error("Error retrieving data:", error);
                });

              available_wallets.push(wallet_array[x]);
              continue;
            }

            console.log(
              `${wallet_array[x].name} ${wallet_array[x].public_key}: Retrying failed transfer ${retries.count}...`
            );

            await retryTransfer.retryTransfer(last_processed[0]);
            continue;
          }

          if (
            last_processed[0].progress !== "PROCESSING" &&
            last_processed[0].progress !== "TRANSFER-FAILED"
          ) {
            available_wallets.push(wallet_array[x]);
          }
        }

        console.log(
          `${network_array[i].network} has ${available_wallets.length} available wallets.`
        );

        if (Number(available_wallets.length) === 0) {
          continue;
        }

        request[0].approver = available_wallets[0].public_key;
        pending_requests.push(request[0]);
      }

      return pending_requests;
    } catch (error) {
      throw new Error("Error fetching pending requests: " + error.message);
    }
  },
};
