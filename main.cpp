#include <iostream>
#include <vector>
#include <string>
#include <clickhouse/client.h>

using namespace clickhouse;

struct Row {
    std::string datetime;
    std::string msgtype;
    std::string severity;
    std::string ownerpermissions;
    std::string operationresult;
    std::string actiontype;
    std::string objectid;
    std::string grouppermissions;
    std::string classifyinglabel;
};

bool compareRows(const Row &a, const Row &b) {
    return a.datetime == b.datetime &&
           a.msgtype == b.msgtype &&
           a.severity == b.severity &&
           a.ownerpermissions == b.ownerpermissions &&
           a.operationresult == b.operationresult &&
           a.actiontype == b.actiontype &&
           a.objectid == b.objectid &&
           a.grouppermissions == b.grouppermissions &&
           a.classifyinglabel == b.classifyinglabel;
}

std::vector<Row> fetchRows(Client &client, const std::string &table_name) {
    std::vector<Row> rows;
    std::string query = "SELECT * FROM " + table_name;

    client.Select(query, [&](const Block &block) {
        std::cout << "Processing block with " << block.GetRowCount() << " rows and " << block.GetColumnCount() << " columns." << std::endl;



        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            Row row;
            try {
                auto col0 = block[0]->As<ColumnDateTime64>();
                auto col1 = block[1]->As<ColumnUInt32>();
                auto col2 = block[2]->As<ColumnNullable>();
                auto col3 = block[3]->As<ColumnNullable>();
                auto col4 = block[4]->As<ColumnNullable>();
                auto col5 = block[5]->As<ColumnNullable>();
                auto col6 = block[6]->As<ColumnNullable>();
                auto col7 = block[7]->As<ColumnNullable>();
                auto col8 = block[8]->As<ColumnNullable>();

                if (col0 && col1 && col2 && col3 && col4 && col5 && col6 && col7 && col8) {
                    row.datetime = std::to_string(col0->At(i));
                    row.msgtype = std::to_string(col1->At(i));
                    row.severity = col2->IsNull(i) ? "" : std::to_string(col2->Nested()->As<ColumnUInt8>()->At(i));
                    row.ownerpermissions = col3->IsNull(i) ? "" : col3->Nested()->As<ColumnString>()->At(i);
                    row.operationresult = col4->IsNull(i) ? "" : std::to_string(col4->Nested()->As<ColumnUInt8>()->At(i));
                    row.actiontype = col5->IsNull(i) ? "" : std::to_string(col5->Nested()->As<ColumnUInt8>()->At(i));
                    row.objectid = col6->IsNull(i) ? "" : col6->Nested()->As<ColumnString>()->At(i);
                    row.grouppermissions = col7->IsNull(i) ? "" : col7->Nested()->As<ColumnString>()->At(i);
                    row.classifyinglabel = col8->IsNull(i) ? "" : col8->Nested()->As<ColumnString>()->At(i);
                } else {
                    std::cerr << "Ошибка: один из столбцов имеет неправильный тип в таблице " << table_name << std::endl;
                    continue;
                }
            } catch (const std::exception &e) {
                std::cerr << "Ошибка при обработке строки " << i << ": " << e.what() << std::endl;
                continue; // Пропустить эту строку
            }
            rows.push_back(row);
        }
    });

    return rows;
}

int main() {
    try {
        ClientOptions options;
        options.SetHost("localhost")
                .SetPort(9000)
                .SetUser("default")
                .SetPassword("")
                .SetDefaultDatabase("default");

        Client client(options);

        auto rows1 = fetchRows(client, "t_accessattributes");
        auto rows2 = fetchRows(client, "t_accessattributes2");

        int matching_rows_count = 0;

        for (const auto &row1: rows1) {
            for (const auto &row2: rows2) {
                if (compareRows(row1, row2)) {
                    ++matching_rows_count;
                }
            }
        }

        std::cout << "Количество совпадающих строк: " << matching_rows_count << std::endl;
    } catch (const std::exception &e) {
        std::cerr << "Исключение: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
