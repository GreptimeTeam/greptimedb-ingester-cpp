#include <iostream>
#include <greptime/v1/column.pb.h>
#include <src/database.h>
void test_hello() {
    greptime::Database database;
    database.hello();
}
int main () {
    test_hello();
    return 0;
}