import { DataTypes, Model, Optional } from "sequelize";
import { DeviceTokenInterface } from "../interfaces/index";
import db from "../../config/db";

interface DeviceTokenCreationModel extends Optional<DeviceTokenInterface, "id"> { }
interface DeviceTokenInstance
  extends Model<DeviceTokenInterface, DeviceTokenCreationModel>,
  DeviceTokenInterface { }

const dataObj = {
  id: {
    primaryKey: true,
    type: DataTypes.INTEGER,
    allowNull: false,
    autoIncrement: true,
  },
  user_id: {
    type: DataTypes.INTEGER,
    allowNull: false,
  },
  device_id: {
    type: DataTypes.STRING,
    allowNull: false,
  },
  device_token: {
    type: DataTypes.TEXT,
    allowNull: false,
  },
  status: {
    type: DataTypes.TINYINT,
    allowNull: true,
  },
  address: {
    type: DataTypes.STRING,
    allowNull: false,
  },
  coin_family: {
    type: DataTypes.TINYINT,
    allowNull: false,
  },
  push: {
    type: DataTypes.INTEGER,
    allowNull: true,
  },
};

const dataObjIndex = {
  indexes: [
    {
      unique: false,
      fields: ["user_id"],
    },
    {
      unique: false,
      fields: ["status"],
    },
    {
      unique: false,
      fields: ["address"],
    },
    {
      unique: false,
      fields: ["coin_family"],
    }
  ],
};

const DeviceTokenModel = db.db_write.define<DeviceTokenInstance>("device_tokens", dataObj, dataObjIndex);

export default DeviceTokenModel;