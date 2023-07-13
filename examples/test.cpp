#include <iostream>
#include <src/database.h>
void test_hello() {
    greptime::Database database;
    database.hello();
}
int main () {

    test_hello();

    return 0;
}