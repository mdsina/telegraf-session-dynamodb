const DynamoDBHelper = require('./dynamodb-helper');
const lzma = require('lzma-native');

class DynamoDBSession {
    constructor(options) {
        this.options = Object.assign({
            property: 'session',
            getSessionKey: (ctx) => ctx.from && ctx.chat && `${ctx.from.id}:${ctx.chat.id}`,
            dynamoDBConfig: {
                params: {
                    TableName: 'telegraf-session-dynamodb' // override this value to your table
                }
            },
            compression: {
                enabled: false,
                level: 9,
                compressSession: (sessionJsonString, level) => lzma.compress(sessionJsonString, level),
                decompressSession: (sessionJsonString, level) => lzma.decompress(sessionJsonString, level)
            }
        }, options);

        this._db = new DynamoDBHelper(this.options.dynamoDBConfig);
    }

    _packValue(value) {
        if (this.options.compression.enabled) {
            return this.options.compression
                .compressSession(JSON.stringify(value), this.options.compression.level)
                .then(buffer => new Object({
                    B: buffer
                }));
        }

        return Promise.resolve(value);
    }

    _unpackValue(value) {
        console.log(value);
        if (this.options.compression.enabled) {
            return this.options.compression
                .decompressSession(Buffer.from(value, 'base64'), this.options.compression.level)
                .then(data => JSON.parse(data.toString()));
        }
        return Promise.resolve(value);
    }

    createSession(key) {
        return this._packValue({}).then(value => {
            let params = {
                Item: {
                    SessionKey: key,
                    SessionValue: value
                }
            };
            return this._db.create(params);
        }).catch((err) => console.log(err));
    }

    getSession(key) {
        let params = {
            Key: {
                SessionKey: key
            }
        };
        return this._db.read(params)
            .then((data) => {
                if (!data.Item || Object.keys(data.Item).length === 0) {
                    return this.createSession(key)
                        .then(() => {})
                        .catch((err) => console.log(err));
                }
                return data.Item.SessionValue;
            })
            .then(data => this._unpackValue(data))
            .catch((err) => console.log(err));
    }

    saveSession(key, session) {
        if (!session || Object.keys(session).length === 0) {
            return this.clearSession(key);
        }
        this._packValue(session).then(value => {
            let params = {
                Key: {
                    SessionKey: key
                },
                UpdateExpression: 'set SessionValue = :v',
                ExpressionAttributeValues: {
                    ':v': value
                }
            };

            return this._db.update(params);
        }).catch((err) => console.log(err));
    }

    clearSession(key) {
        let params = {
            Key: {
                SessionKey: key
            }
        };
        return this._db.delete(params)
            .catch((err) => console.log(err));
    }

    middleware() {
        return (ctx, next) => {
            const key = this.options.getSessionKey(ctx);
            if (!key) {
                return next();
            }
            return this.getSession(key)
                .then((session) => {
                    Object.defineProperty(ctx, this.options.property, {
                        get: () => session,
                        set: newValue => {
                            session = Object.assign({}, newValue);
                        }
                    });
                    return next().then(() => this.saveSession(key, session));
                }).catch((err) => console.log(err));
        }
    }
}

module.exports = DynamoDBSession;