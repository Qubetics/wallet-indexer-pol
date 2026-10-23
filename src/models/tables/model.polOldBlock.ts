import { DataTypes, Model, Optional } from "sequelize";
import { PolOldBlockInterface } from "../interfaces/index";
import db from "../../config/db";

interface PolOldBlockCreationModel extends Optional<PolOldBlockInterface, "id"> { }
interface PolOldBlockInstance
    extends Model<PolOldBlockInterface, PolOldBlockCreationModel>,
    PolOldBlockInterface { }

const dataObj = {
    id: {
        primaryKey: true,
        type: DataTypes.INTEGER,
        allowNull: false,
        autoIncrement: true,
    },
    block_number: {
        type: DataTypes.INTEGER,
        allowNull: false,
    },
    start_block: {
        type: DataTypes.INTEGER,
        allowNull: true,
    },
    end_block: {
        type: DataTypes.INTEGER,
        allowNull: true,
    },
    coin_family: {
        type: DataTypes.INTEGER,
        allowNull: false,
    },
    status: {
        type: DataTypes.TINYINT,
        allowNull: true
    }
};
let dataObjIndex = {
    createdAt: 'created_at',
    updatedAt: 'updated_at',
    indexes: [
        {
            unique: false,
            fields: ["status"],
        }
    ]
}


const polOldBLockModel = db.db_write.define<PolOldBlockInstance>("pol_old_blocks", dataObj, dataObjIndex);

export default polOldBLockModel;