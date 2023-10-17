require("dotenv").config();
const mysql = require("mysql");
const DKGClient = require("dkg.js");

const othubdb_connection = mysql.createConnection({
  host: process.env.DBHOST,
  user: process.env.DBUSER,
  password: process.env.DBPASSWORD,
  database: process.env.OTHUB_DB,
});

const OT_NODE_TESTNET_PORT = process.env.OT_NODE_TESTNET_PORT;
const OT_NODE_MAINNET_PORT = process.env.OT_NODE_MAINNET_PORT;

const testnet_node_options = {
  endpoint: process.env.OT_NODE_HOSTNAME,
  port: OT_NODE_TESTNET_PORT,
  useSSL: true,
  maxNumberOfRetries: 100,
};

const mainnet_node_options = {
  endpoint: process.env.OT_NODE_HOSTNAME,
  port: OT_NODE_MAINNET_PORT,
  useSSL: true,
  maxNumberOfRetries: 100,
};

const testnet_dkg = new DKGClient(testnet_node_options);
const mainnet_dkg = new DKGClient(mainnet_node_options);

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

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

// async function retryTransfer(data) {
//   try {
//     const wallet_array = JSON.parse(process.env.WALLET_ARRAY);
//     let index = wallet_array.findIndex(
//       (obj) => obj.public_key == data.approver
//     );
//     console.log(
//       `${wallet_array[index].name} wallet ${wallet_array[index].public_key}: Retrying Transfer for UAL ${data.ual} on ${data.network}.`
//     );

//     if (data.network === "otp::testnet") {
//       await testnet_dkg.asset
//         .transfer(dkg_create_result.UAL, data.receiver, {
//           maxNumberOfRetries: 30,
//           frequency: 2,
//           contentType: "all",
//           blockchain: {
//             name: data.network,
//             publicKey: wallet_array[index].public_key,
//             privateKey: wallet_array[index].private_key,
//           },
//         })
//         .then(async (result) => {
//           console.log(
//             `${wallet_array[index].name} wallet ${wallet_array[index].public_key}: Transfered ${dkg_create_result.UAL} to ${data.receiver}.`
//           );

//           query = `UPDATE txn_header SET progress = ?, ual = ?, state = ? WHERE  txn_id = ?`;
//           params = [
//             "COMPLETE",
//             data.UAL,
//             dkg_create_result.publicAssertionId,
//             data.txn_id,
//           ];
//           await getOTHubData(query, params)
//             .then((results) => {
//               //console.log('Query results:', results);
//               return results;
//               // Use the results in your variable or perform further operations
//             })
//             .catch((error) => {
//               console.error("Error retrieving data:", error);
//             });

//           return result;
//         })
//         .catch(async (error) => {
//           console.log(error);
//           console.log(
//             `${wallet_array[index].name} wallet ${wallet_array[index].public_key}: Transfer failed. Inserting failed transfer record...`
//           );

//           query = `INSERT INTO txn_header (txn_id, progress, approver, api_key, request, network, app_name, txn_description, txn_data, ual, keywords, state, txn_hash, txn_fee, trac_fee, epochs, receiver) VALUES (UUID(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`;
//           params = [
//             "TRANSFER-FAILED",
//             wallet_array[index].public_key,
//             null,
//             "Create-N-Transfer",
//             data.network,
//             null,
//             null,
//             null,
//             dkg_create_result.UAL,
//             null,
//             null,
//             null,
//             null,
//             null,
//             null,
//             data.receiver,
//           ];
//           await getOTHubData(query, params)
//             .then((results) => {
//               //console.log('Query results:', results);
//               return results;
//               // Use the results in your variable or perform further operations
//             })
//             .catch((error) => {
//               console.error("Error retrieving data:", error);
//             });

//           throw new Error("Transfer failed: " + error.message);
//         });
//     }

//   } catch (error) {
//     console.error("Error processing pending uploads:", error);
//   }
// }

async function uploadData(data) {
  try {
    let query = `UPDATE txn_header SET progress = ?, approver = ? WHERE txn_id = ?`;
    let params = ["PROCESSING", data.approver, data.txn_id];
    await getOTHubData(query, params)
      .then((results) => {
        //console.log('Query results:', results);
        return results;
        // Use the results in your variable or perform further operations
      })
      .catch((error) => {
        console.error("Error retrieving data:", error);
      });

    let dkg_txn_data = JSON.parse(data.txn_data);
    if (!dkg_txn_data["@context"]) {
      dkg_txn_data["@context"] = "https://schema.org";
    }

    const wallet_array = JSON.parse(process.env.WALLET_ARRAY);
    let index = wallet_array.findIndex(
      (obj) => obj.public_key == data.approver
    );

    console.log(
      `${wallet_array[index].name} wallet ${wallet_array[index].public_key}: Creating next asset on ${data.network}.`
    );

    if (data.network === "otp::testnet") {
      let dkg_create_result = await testnet_dkg.asset
        .create(
          {
            public: dkg_txn_data,
          },
          {
            epochsNum: data.epochs,
            maxNumberOfRetries: 30,
            frequency: 2,
            contentType: "all",
            keywords: data.keywords,
            blockchain: {
              name: data.network,
              publicKey: wallet_array[index].public_key,
              privateKey: wallet_array[index].private_key,
              handleNotMinedError: true
            },
          }
        )
        .then((result) => {
          //console.log(JSON.stringify(result))
          return result;
        })
        .catch(async (error) => {
          console.log(error);
          console.log(
            `${wallet_array[index].name} wallet ${wallet_array[index].public_key}: Create failed. Setting back to pending in 3 minutes...`
          );
          await sleep(180000);

          query = `UPDATE txn_header SET progress = ?, approver = ? WHERE txn_id = ?`;
          params = ["PENDING", null, data.txn_id];
          await getOTHubData(query, params)
            .then((results) => {
              //console.log('Query results:', results);
              return results;
              // Use the results in your variable or perform further operations
            })
            .catch((error) => {
              console.error("Error retrieving data:", error);
            });

          throw new Error("Create failed: " + error.message);
        });

      console.log(
        `${wallet_array[index].name} wallet ${wallet_array[index].public_key}: Created UAL ${dkg_create_result.UAL}. Transfering to ${data.receiver}...`
      );

      await testnet_dkg.asset
        .transfer(dkg_create_result.UAL, data.receiver, {
          epochsNum: data.epochs,
          maxNumberOfRetries: 30,
          frequency: 2,
          contentType: "all",
          keywords: data.keywords,
          blockchain: {
            name: data.network,
            publicKey: wallet_array[index].public_key,
            privateKey: wallet_array[index].private_key,
            handleNotMinedError: true
          },
        })
        .then(async (result) => {
          console.log(
            `${wallet_array[index].name} wallet ${wallet_array[index].public_key}: Transfered ${dkg_create_result.UAL} to ${data.receiver}.`
          );

          query = `UPDATE txn_header SET progress = ?, ual = ?, state = ? WHERE  txn_id = ?`;
          params = [
            "COMPLETE",
            dkg_create_result.UAL,
            dkg_create_result.publicAssertionId,
            data.txn_id,
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

          return result;
        })
        .catch(async (error) => {
          console.log(error);
          console.log(
            `${wallet_array[index].name} wallet ${wallet_array[index].public_key}: Transfer failed. Inserting failed transfer record...`
          );

          query = `INSERT INTO txn_header (txn_id, progress, approver, api_key, request, network, app_name, txn_description, txn_data, ual, keywords, state, txn_hash, txn_fee, trac_fee, epochs, receiver) VALUES (UUID(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`;
          params = [
            "TRANSFER-FAILED",
            wallet_array[index].public_key,
            null,
            "Create-N-Transfer",
            data.network,
            null,
            null,
            null,
            dkg_create_result.UAL,
            null,
            null,
            null,
            null,
            null,
            null,
            data.receiver,
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

          throw new Error("Transfer failed: " + error.message);
        });
    }

    return;
  } catch (error) {
    //throw new Error("Unexpected Error: " + error.message);
  }
}

async function getPendingUploadRequests() {
  try {
    console.log(`Checking for transactions to process...`);
    const network_array = JSON.parse(process.env.SUPPORTED_NETWORKS);
    const wallet_array = JSON.parse(process.env.WALLET_ARRAY);

    let pending_requests = [];
    for (i = 0; i < network_array.length; i++) {
      sqlQuery =
        "select * FROM txn_header where progress = ? and network = ? ORDER BY created_at ASC LIMIT 1";
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
        console.log(
          `${network_array[i].network} has no pending requests.`
        );
        continue;
      }

      let available_wallets = [];
      for (x = 0; x < wallet_array.length; x++) {
        query = `select * from txn_header where request = 'Create-n-Transfer' AND approver = ? AND network = ? order by updated_at desc LIMIT 5`;
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

        if(Number(last_processed.length) === 0){
          available_wallets.push(wallet_array[x].public_key);
          continue;
        }

        let updatedAtTimestamp = last_processed[0].updated_at;
        let currentTimestamp = new Date();
        let timeDifference = currentTimestamp - updatedAtTimestamp;

        if (
          last_processed[0].progress === "PROCESSING" &&
          timeDifference >= 300000
        ) {
          console.log(`${wallet_array[x].name} ${wallet_array[x].public_key}: Processing for over 5 minutes. Rolling back to PENDING...`)
          query = `UPDATE txn_header SET progress = ?, approver = ? WHERE approver = ? AND progress = ?`;
          params = ["PENDING", null, wallet_array[x].public_key, "PROCESSING"];
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

        // if (
        //   last_processed[0].progress === "TRANSFER-FAILED" && 
        //   last_processed[1].progress === "TRANSFER-FAILED" && 
        //   last_processed[2].progress === "TRANSFER-FAILED" && 
        //   last_processed[3].progress === "TRANSFER-FAILED" && 
        //   last_processed[4].progress === "TRANSFER-FAILED"
        // ) {
        //   console.log(`${wallet_array[x].name} ${wallet_array[x].public_key}: Transfer attempt failed 5 times. Abandoning transfer...`)
        //   query = `UPDATE txn_header SET progress = ?, approver = ? WHERE txn_id = ?`;
        //   params = ["ABANDONED", null, last_processed[0].txn_id];
        //   await getOTHubData(query, params)
        //     .then((results) => {
        //       //console.log('Query results:', results);
        //       return results;
        //       // Use the results in your variable or perform further operations
        //     })
        //     .catch((error) => {
        //       console.error("Error retrieving data:", error);
        //     });

        //   available_wallets.push(wallet_array[x].public_key);
        //   continue;
        // }

        // if (
        //   last_processed[0].progress === "TRANSFER-FAILED"
        // ) {
        //   console.log(`${wallet_array[x].name} ${wallet_array[x].public_key}: Retrying failed Transfer...`)

        //   await retryTransfer(last_processed[0]);
        //   continue;
        // }

        if (
          last_processed[0].progress !== "PROCESSING"
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
}

async function processPendingUploads() {
  try {
    setTimeout(processPendingUploads, process.env.CYCLE_TIME_SEC * 1000);
    const pending_requests = await getPendingUploadRequests();

    if (Number(pending_requests.length) === 0) {
      return;
    }

    const promises = pending_requests.map((request) => uploadData(request));

    const wallet_array = JSON.parse(process.env.WALLET_ARRAY);
    const concurrentUploads = wallet_array.length + 100;
    await Promise.all(promises.slice(0, concurrentUploads));
  } catch (error) {
    console.error("Error processing pending uploads:", error);
  }
}

processPendingUploads();
