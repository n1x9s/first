#include <clickhouse/client.h>
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>
#include <stdexcept>
#include <unordered_map>

using namespace clickhouse;
using namespace std;

using Col = pair<string, string>;
using TblCol = vector<Col>;
using Tbl = pair<string, TblCol>;
using Db = vector<Tbl>;

// Реализация функции join
string join(const vector<string>& elements, const string& delimiter) {
    ostringstream os;
    for (auto it = elements.begin(); it != elements.end(); ++it) {
        if (it != elements.begin()) {
            os << delimiter;
        }
        os << *it;
    }
    return os.str();
}

// Проверка соответствия типов столбцов
bool validateColumnTypes(const TblCol& expectedColumns, const TblCol& actualColumns) {
    if (expectedColumns.size() != actualColumns.size()) {
        return false;
    }

    for (size_t i = 0; i < expectedColumns.size(); ++i) {
        if (expectedColumns[i].first != actualColumns[i].first ||
            expectedColumns[i].second != actualColumns[i].second) {
            return false;
        }
    }

    return true;
}

int main() {
    ClientOptions options;
    options.SetHost("localhost");

    Client client(options);

    vector<pair<string, string>> expectedColumns = {
            {"datetime", "DateTime64(3)"},
            {"msgtype", "UInt32"},
            {"severity", "Nullable(UInt8)"},
            {"ownerpermissions", "Nullable(String)"},
            {"operationresult", "Nullable(UInt8)"},
            {"actiontype", "Nullable(UInt8)"},
            {"objectid", "Nullable(String)"},
            {"grouppermissions", "Nullable(String)"},
            {"classifyinglabel", "Nullable(String)"}
    };

    string table_name = "t_accessattributes";

    // Запрос для получения типов столбцов из ClickHouse
    string query = "SELECT name, type FROM system.columns WHERE table = '" + table_name + "'";

    // Выполнение запроса и получение типов столбцов
    vector<pair<string, string>> actualColumns;
    client.Select(query, [&](const Block& block) {
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            string name(block[0]->As<ColumnString>()->At(i)); // Преобразование string_view в string
            string type(block[1]->As<ColumnString>()->At(i)); // Преобразование string_view в string
            actualColumns.emplace_back(name, type);
        }
    });

    // Проверка типов столбцов
    if (!validateColumnTypes(expectedColumns, actualColumns)) {
        cerr << "Ошибка: Типы столбцов в таблице не соответствуют ожидаемым типам." << endl;
        return 1;
    }

    vector<string> values;
    for (const auto& col : expectedColumns) {
        string value;
        cout << col.first << " (" << col.second << "): ";
        getline(cin, value);

        // Удаление пробелов в начале и в конце строки
        value.erase(0, value.find_first_not_of(" \t\n\r\f\v"));
        value.erase(value.find_last_not_of(" \t\n\r\f\v") + 1);

        if (col.second == "DateTime64(3)") {
            // Проверка формата даты и времени
            struct tm tm = {};
            stringstream ss(value);
            ss >> get_time(&tm, "%Y-%m-%d %H:%M:%S");
            if (ss.fail()) {
                cerr << "Ошибка: Неверный формат даты и времени для значения " << value << ". Ожидаемый формат: YYYY-MM-DD HH:MM:SS" << endl;
                return 1;
            }
            value = to_string(mktime(&tm)) + ".000";  // преобразование в timestamp и добавление миллисекунд
        } else if (col.second == "UInt32") {
            try {
                uint32_t val = stoul(value);
                value = to_string(val);
            } catch (const invalid_argument& e) {
                cerr << "Ошибка: Столбец " << col.first << " имеет неверный тип для значения " << value << ". Ожидаемый тип: " << col.second << "." << endl;
                return 1;
            }
        } else if (col.second == "Nullable(UInt8)") {
            try {
                if (!value.empty()) {
                    uint8_t val = static_cast<uint8_t>(stoul(value));
                    value = to_string(val);
                } else {
                    value = "NULL";
                }
            } catch (const invalid_argument& e) {
                cerr << "Ошибка: Столбец " << col.first << " имеет неверный тип для значения " << value << ". Ожидаемый тип: " << col.second << "." << endl;
                return 1;
            }
        } else if (col.second == "Nullable(String)") {
            if (value.empty()) {
                value = "NULL";
            } else {
                value = "'" + value + "'";
            }
        }

        values.push_back(value);
    }

    try {
        string query = "INSERT INTO " + table_name + " VALUES (" + join(values, ", ") + ")";
        client.Execute(query);
        cout << "Данные успешно вставлены." << endl;
    } catch (const ServerException& e) {
        cerr << "Ошибка: " << e.what() << endl;
        return 1;
    }

    return 0;
}
