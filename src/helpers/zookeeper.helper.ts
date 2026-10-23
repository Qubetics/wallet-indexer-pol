import zookeeper from 'node-zookeeper-client';
import { config } from 'dotenv';
config({ quiet: true });
const client = zookeeper.createClient(
    process.env.ZOOKEEPER_HOST ||
    'localhost:2181'
);
class ZookeeprProvider {
    config: any = {};
    constructor() {
        client.connect();
    }

    connectZookeeper = async () => {

        return new Promise((resolve, reject) => {
            client.once('connected', () => {

                client.getData(
                    process.env.ZOOKEEPER_PATH || '/qubetics_wallet',
                    (err: any, data: any, _: any) => {
                        if (err) {
                            console.error(`connectZookeeper err...`, err);
                            reject(err);
                        }
                        if (data) {
                            this.config = JSON.parse(data.toString('utf8'));
                        }
                        resolve(true);
                    }
                );
            });
        });
    }
}

const zookeeperConfig = new ZookeeprProvider();

export default zookeeperConfig;