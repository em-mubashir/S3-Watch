const {
  S3Client,
  ListObjectsCommand,
  GetObjectCommand,
  DeleteObjectCommand,
} = require("@aws-sdk/client-s3");
const fs = require("fs");
const path = require("path");
const pool = require("../config/db");
const { createMultiWatch } = require("../config/aws");

class S3Watcher {
  async init(channelName) {
    this.config = await getAwsConfig(channelName);
    this.bucketName = this.config.bucketName;
    this.prefix = this.config.destinationFolder;
    // this.fileType = ".xml";
    this.credentials = {
      accessKeyId: this.config.accessKeyId,
      secretAccessKey: this.config.secretAccessKey,
    };
    this.s3 = new S3Client({
      region: this.config.region,
      credentials: this.credentials,
    });
    const [rows] = await pool.query(
      `SELECT * FROM prodenvironment where CHANNEL_NAME='${channelName}'`
    );
    if (rows[0].CHNNEL_NAME !== 0) {
      //   console.log("this.fileType", rows[0].FILEEXTENSION);
      this.fileType = rows[0].FILEEXTENSION;
      this.groupId = rows[0].GROUPID;
      this.companyId = rows[0].COMPANYID;
      this.path = rows[0].WATCHPARAMS;
    }
  }

  async watch(onChange) {
    await this.init("s32");
    const currentObjects = await this.s3.send(
      new ListObjectsCommand({ Bucket: this.bucketName, Prefix: this.prefix })
    );
    this.currentKeys = new Set(currentObjects.Contents.map((obj) => obj.Key));

    this.currentVersion = currentObjects.VersionIdMarker || null;
    while (true) {
      const result = await this.s3.send(
        new ListObjectsCommand({
          Bucket: this.bucketName,
          Prefix: this.prefix,
          VersionIdMarker: this.currentVersion,
          Waiter: "s3:ObjectCreated:*",
        })
      );

      let newObjects = result.Contents.filter((obj) => {
        if (
          this.fileType === "*.*" &&
          obj.Key.toLowerCase() !== this.prefix.toLowerCase()
        ) {
          return obj.Key.toLowerCase().startsWith(this.prefix.toLowerCase());
        } else {
          return obj.Key.toLowerCase().endsWith(this.fileType.toLowerCase());
        }
      }).map((obj) => ({
        name: obj.Key.slice(this.prefix.length),
        key: obj.Key,
      }));

      if (newObjects.length > 0) {
        onChange(newObjects);
        newObjects.forEach((obj) => this.currentKeys.add(obj.Key));
      }

      this.currentVersion = result.NextVersionIdMarker || this.currentVersion;
    }
  }

  async process(newObjects) {
    newObjects.forEach(async (obj) => {
      const params = {
        Bucket: this.bucketName,
        Key: obj.key,
      };
      const fileStream = fs.createWriteStream(
        `${process.env.TangoData}\\${this.companyId}\\${this.groupId}\\work\\${this.path}\\${obj.name}`
      );

      try {
        const getObjectCommand = new GetObjectCommand(params);
        const { Body } = await this.s3.send(getObjectCommand);
        Body.pipe(fileStream);
        console.log(
          `Downloaded ${obj.name} to local folder. ${process.env.TangoData}\\${this.companyId}\\${this.groupId}\\work\\${this.path}\\${obj.name}`
        );
        const param2 = {
          Bucket: this.bucketName,
          Key: obj.key,
        };
        const deleteObjectCommand = new DeleteObjectCommand(param2);
        await this.s3.send(deleteObjectCommand);
        console.log(`Deleted ${obj.name} from S3 bucket.`);
      } catch (err) {
        console.log(`Failed to download ${obj.name}: ${err.message}`);
      }
    });
  }
}

module.exports = S3Watcher;
