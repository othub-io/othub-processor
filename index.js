require("dotenv").config();
const uploadData = require('./src/uploadData.js');
const getPendingUploadRequests = require('./src/getPendingUploadRequests.js');
const wallet_array = JSON.parse(process.env.WALLET_ARRAY);

async function processPendingUploads() {
  try {
    setTimeout(processPendingUploads, process.env.CYCLE_TIME_SEC * 1000);
    const pending_requests = await getPendingUploadRequests.getPendingUploadRequests();

    if (Number(pending_requests.length) === 0) {
      return;
    }

    const promises = pending_requests.map((request) => uploadData.uploadData(request));

    const concurrentUploads = wallet_array.length + 100;
    await Promise.all(promises.slice(0, concurrentUploads));
  } catch (error) {
    console.error("Error processing pending uploads:", error);
  }
}

processPendingUploads();
