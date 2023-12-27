#include "lib.hpp"

std::string itos(int x) {
    std::stringstream ss;
    return (std::stringstream() << x).str();
}

int getPort(int id) {
    return 4042 + id * 2;
}
std::string getAddr(int port) {
    std::stringstream ss;
    ss << "tcp://127.0.0.1:" << port;

    return ss.str();
}
std::string getAddrPrev(int id) {
    return getAddr( getPort(id) + 1 );
}
std::string getAddrNext(int id) {
    return getAddr( getPort(id) );
}