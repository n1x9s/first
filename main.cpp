#include <clickhouse/client.h>
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>
#include <stdexcept>
#include <unordered_map>
#include "schemas.h"

using namespace clickhouse;
using namespace std;

using Col = pair<string, string>;
using TblCol = vector<Col>;
using Tbl = pair<string, TblCol>;
using Db = vector<Tbl>;

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

vector<string> getTables(Client& client) {
    vector<string> tables;
    client.Select("SHOW TABLES", [&](const Block& block) {
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            string table(block[0]->As<ColumnString>()->At(i));
            tables.push_back(table);
        }
    });
    return tables;
}

TblCol getTableSchema(Client& client, const string& table_name) {
    TblCol columns;
    string query = "SELECT name, type FROM system.columns WHERE table = '" + table_name + "'";
    client.Select(query, [&](const Block& block) {
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            string name(block[0]->As<ColumnString>()->At(i));
            string type(block[1]->As<ColumnString>()->At(i));
            columns.emplace_back(name, type);
        }
    });
    return columns;
}

bool compareSchema(const TblCol& actual, const TblCol& expected) {
    if (actual.size() != expected.size()) {
        cerr << "Ошибка: Количество столбцов не совпадает." << endl;
        return false;
    }

    for (size_t i = 0; i < actual.size(); ++i) {
        if (actual[i].first != expected[i].first || actual[i].second != expected[i].second) {
            cerr << "Ошибка: Несоответствие в столбце '" << actual[i].first << "'. Ожидаемый тип: "
                 << expected[i].second << ", фактический тип: " << actual[i].second << "." << endl;
            return false;
        }
    }

    return true;
}

int main() {
    ClientOptions options;
    options.SetHost("localhost");

    Client client(options);

    vector<string> tables = getTables(client);

    if (tables.empty()) {
        cerr << "Ошибка: В базе данных нет таблиц." << endl;
        return 1;
    }

    cout << "Доступные таблицы:" << endl;
    for (const auto& table : tables) {
        cout << "- " << table << endl;
    }

    string table_name;
    cout << "Введите имя таблицы: ";
    getline(cin, table_name);

    if (find(tables.begin(), tables.end(), table_name) == tables.end()) {
        cerr << "Ошибка: Таблица с именем '" << table_name << "' не найдена." << endl;
        return 1;
    }

    TblCol actualColumns = getTableSchema(client, table_name);
    unordered_map<string, TblCol> schemas = getSchemas();

    if (schemas.find(table_name) == schemas.end()) {
        cerr << "Ошибка: Нет эталонной схемы для таблицы '" << table_name << "'." << endl;
        return 1;
    }

    TblCol expectedColumns = schemas[table_name];

    if (!compareSchema(actualColumns, expectedColumns)) {
        return 1;
    }

    vector<string> values;
    for (const auto& col : actualColumns) {
        string value;
        cout << col.first << " (" << col.second << "): ";
        getline(cin, value);

        value.erase(0, value.find_first_not_of(" \t\n\r\f\v"));
        value.erase(value.find_last_not_of(" \t\n\r\f\v") + 1);

        try {
            if (col.second == "DateTime64(3)") {
                struct tm tm = {};
                stringstream ss(value);
                ss >> get_time(&tm, "%Y-%m-%d %H:%M:%S");
                if (ss.fail()) {
                    cerr << "Ошибка: Неверный формат даты и времени для значения " << value << ". Ожидаемый формат: YYYY-MM-DD HH:MM:SS" << endl;
                    return 1;
                }
                value = to_string(mktime(&tm)) + ".000";
            } else if (col.second.find("UInt32") != string::npos) {
                uint32_t val = stoul(value);
                value = to_string(val);
            } else if (col.second.find("Nullable(UInt8)") != string::npos) {
                if (!value.empty()) {
                    uint8_t val = static_cast<uint8_t>(stoul(value));
                    value = to_string(val);
                } else {
                    value = "NULL";
                }
            } else if (col.second.find("Nullable(String)") != string::npos) {
                if (value.empty()) {
                    value = "NULL";
                } else {
                    value = "'" + value + "'";
                }
            } else if (col.second.find("String") != string::npos) {
                value = "'" + value + "'";
            } else {
                value = value;
            }
        } catch (const invalid_argument& e) {
            cerr << "Ошибка: Столбец " << col.first << " имеет неверный тип для значения " << value << ". Ожидаемый тип: " << col.second << "." << endl;
            return 1;
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
