package esb.bundles.core.audit.db;

import db.ColumnOptions;
import db.ColumnType;
import promises.PromiseUtils;
import esb.core.Message;
import db.Record;
import esb.core.config.sections.EsbConfig;
import db.DatabaseFactory;
import db.IDatabase;
import esb.core.IBundle;
import promises.Promise;
import esb.common.Uri;
import esb.core.IConsumer;
import esb.core.Bus.*;

using StringTools;

@:keep
class AuditDbConsumer implements IConsumer {
    public var bundle:IBundle;
    public function start(uri:Uri) {
        from(uri, (uri, message) -> {
            return new Promise((resolve, reject) -> {
                var dbName = EsbConfig.get().path(uri.domain + "-audit.db", false);
                var db:IDatabase = DatabaseFactory.instance.createDatabase(DatabaseFactory.SQLITE, {
                    filename: dbName
                });

                var newProperties:Map<String, Any> = [];
                for (key in message.properties.keys()) {
                    if (key.startsWith("$$") && key.endsWith("$$")) {
                        newProperties.set(key.substring(2, key.length - 2), message.properties.get(key));
                    }
                }
                message.properties = newProperties;
                message.properties.set("audit.skip", true);

                var hash = message.hash();
                var bodyHash = message.bodyHash();
                var auditRecord = new Record();
                auditRecord.field("timestamp", message.properties.get("audit.timestamp"));
                auditRecord.field("hash", hash);
                auditRecord.field("messageId", message.properties.get(Message.PropertyMessageId));
                auditRecord.field("breadcrumbs", message.breacrumbs().join("|"));
                auditRecord.field("correlationId", message.correlationId);
                auditRecord.field("bodyHash", bodyHash);
                auditRecord.field("properties", mapToString(message.properties));
                auditRecord.field("headers", mapToString(message.headers));

                db.connect().then(result -> {
                    return db.table("messageAudit");
                }).then(result -> {
                    if (!result.table.exists) {
                        return db.createTable("messageAudit", [
                            { name: "messageAuditId", type: ColumnType.Number, options: [ColumnOptions.PrimaryKey, ColumnOptions.NotNull, ColumnOptions.AutoIncrement]},
                            { name: "timestamp", type: ColumnType.Text(32)},
                            { name: "hash", type: ColumnType.Text(32)},
                            { name: "messageId", type: ColumnType.Text(32)},
                            { name: "breadcrumbs", type: ColumnType.Text(255)},
                            { name: "correlationId", type: ColumnType.Text(32)},
                            { name: "bodyHash", type: ColumnType.Text(32)},
                            { name: "properties", type: ColumnType.Memo},
                            { name: "headers", type: ColumnType.Memo}
                        ]);
                    }
                    return PromiseUtils.promisify(result);
                }).then(result -> {
                    return result.table.add(auditRecord);
                }).then(result -> {
                }).then(result -> {
                    db.disconnect();
                }).then(result -> {
                    resolve(message);
                }, error -> {
                    trace("error", error);
                    db.disconnect();
                    resolve(message);
                });
            });
        });
    }

    private static function mapToString(map:Map<String, Any>):String {
        if (map == null) {
            return "";
        }
        var s = "";
        for (key in map.keys()) {
            var value = map.get(key);
            s += key + "|" + Std.string(value) + "||";
        }
        return s;
    }
}