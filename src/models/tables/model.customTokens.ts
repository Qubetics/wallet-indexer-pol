import { DataTypes, Model, Optional } from "sequelize";
import { CustomTokenInterface } from "../interfaces/interface.customTokens";
import db from "../../config/db";

interface CustomTokenCreationModel extends Optional<CustomTokenInterface, "id"> { }
interface CustomTokenInstance extends Model<CustomTokenInterface, CustomTokenCreationModel>, CustomTokenInterface { }

const dataObj = {
    id: {
        type: DataTypes.INTEGER,
        allowNull: false,
        primaryKey: true,
        autoIncrement: true,
    },
    user_id: {
        type: DataTypes.INTEGER,
        allowNull: false,
    },
    coin_id: {
        type: DataTypes.INTEGER,
        allowNull: false,
    },
    created_at: {
        type: DataTypes.DATE,
        allowNull: true,
    },
    updated_at: {
        type: DataTypes.DATE,
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
            fields: ["coin_id"],
        },
    ],
};


const CustomTokennModel = db.db_write.define<CustomTokenInstance>("custom_tokens", dataObj, dataObjIndex);

export default CustomTokennModel;