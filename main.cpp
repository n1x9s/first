#include <clickhouse/client.h>
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>
#include <stdexcept>

using namespace clickhouse;
using namespace std;

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

int main() {
    ClientOptions options;
    options.SetHost("localhost");

    Client client(options);

    vector<pair<string, string>> columns = {
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
    vector<string> values;

    for (const auto& col : columns) {
        string value;
        cout << col.first << " (" << col.second << "): ";
        getline(cin, value);

        value.erase(0, value.find_first_not_of(" \t\n\r\f\v"));
        value.erase(value.find_last_not_of(" \t\n\r\f\v") + 1);

        if (col.second == "DateTime64(3)") {
            struct tm tm = {};
            stringstream ss(value);
            ss >> get_time(&tm, "%Y-%m-%d %H:%M:%S");
            if (ss.fail()) {
                cerr << "Error: Неправильный формат DATETIME " << value << ". Ожидаемый формат YYYY-MM-DD HH:MM:SS" << endl;
                return 1;
            }
            value = to_string(mktime(&tm)) + ".000";
        } else if (col.second == "UInt32") {
            try {
                uint32_t val = stoul(value);
                value = to_string(val);
            } catch (const invalid_argument& e) {
                cerr << "Error: Столбец " << col.first << " имеет неправильный формат " << value << ". Ожидаемый формат " << col.second << "." << endl;
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
                cerr << "Error: Столбец " << col.first << " имеет неправильный формат " << value << ". Ожидаемый формат " << col.second << "." << endl;
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
        cerr << "Error: " << e.what() << endl;
        return 1;
    }

    return 0;
}
