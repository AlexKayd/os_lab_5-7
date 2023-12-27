#include <iostream>
#include <zmq.hpp>
#include <list>
#include <map>
#include <thread>
#include <mutex>
#include <unistd.h>
#include <signal.h>
#include "lib.hpp"

using namespace zmq;
const int MAX_LEN_MSG = 256;
std::mutex print_mutex;

void print(std::vector<std::string> vec) {
    std::lock_guard<std::mutex> lock(print_mutex);
    for (auto elem : vec)
        std::cout << elem;
}

int create_process() {
    pid_t pid = fork();
    if (-1 == pid) {
        perror("fork");
        exit(-1);
    }
    return pid;
}

void kill_process(pid_t pid) {
    kill(pid, SIGTERM);
}

struct Node {
    int id;
    bool isChild;
    int idParent;
    pid_t pid;
};

std::ostream& operator<<(std::ostream& out, Node& node) {
    return out;
}

void changeMainCalcNode(socket_t &pub, int &idMainCalcNode, int id) {
    pub.bind(getAddrNext(id));
    idMainCalcNode = id;
}

void sendMsg(std::string msg, socket_t &pub) {
    mutable_buffer mbuf = buffer(msg);
    auto res_s = pub.send(mbuf, send_flags::none);
    if (!res_s.has_value()) {
        print({"Send error"});
        return;
    }
}

void execThread(std::string msg, socket_t &pub, socket_t &sub, int id) {
    mutable_buffer mbuf = buffer(msg);
    auto res_s = pub.send(mbuf, send_flags::none);
    if (!res_s.has_value()) {
        print({"Send error"});
        return;
    }

    message_t response(msg.size());
    auto res_r = sub.recv(response, recv_flags::none);
    if (!res_r.has_value()) {
        print({"Get error"});
        return;
    }

    std::string res = response.to_string();
    std::stringstream ss;
    ss << "Ok:" << id << ": " << res << "\n";
    print({ss.str()});

    return;
}

std::optional< std::pair<int, int> > killNode(int id, std::list<Node> &nodes, int &idMainCalcNode, socket_t &pub) {
    if (id == -1) {
        return {};
    }
    std::pair<int, int> res;

    auto i = nodes.begin();
    for (; i != nodes.end(); ++i) {
        if (i->id == id) {
            res.first = std::prev(i, 1)->id;
            if (std::next(i, 1) != nodes.end())
                res.second = std::next(i, 1)->id;
            else
                res.second = -2;

            print({"Ok: ", itos(i->pid), "\n"});
            nodes.erase(i);

            auto j = nodes.begin();
            for (; j != nodes.end(); ++j) {
                if (j->isChild && j->idParent == id) {
                    res.second = killNode(j->id, nodes, idMainCalcNode, std::ref(pub))->second;
                    j = nodes.begin();
                }
            }

            break;
        }
    }

    if (i == nodes.end()) {
        return {};
    }

    return res;
}

std::optional< std::pair<int, int> > killNodeFull(int id, std::list<Node> &nodes, int &idMainCalcNode, socket_t &pub, socket_t &sub, std::vector<std::thread> &threads) {
    auto res = killNode(id, nodes, idMainCalcNode, std::ref(pub));
    if (!res)
        return {};

    std::stringstream ss;
    ss << "unbind " << res->first << " " << res->second << " search";
    if (res->first == -1)
        ss << " last";
    else
        ss << " first";
    threads.emplace_back(execThread, ss.str(), std::ref(pub), std::ref(sub), res->first);
    threads.back().join();

    if (id == idMainCalcNode) {
        if (nodes.size() > 1) {
            changeMainCalcNode(std::ref(pub), idMainCalcNode, std::next(nodes.begin(), 1)->id);
        } else {
            idMainCalcNode = -1;
        }
    }

    if (res->first != -1) {
        ss.str("");
        ss << "rebind " << res->first << " next " << res->second;
        sendMsg(ss.str(), pub);
        std::this_thread::sleep_for(std::chrono::duration(std::chrono::milliseconds(100)));
    }

    if (res->second != -2) {
        std::this_thread::sleep_for(std::chrono::duration(std::chrono::milliseconds(100)));
        ss.str("");
        ss.clear();
        ss << "rebind " << res->second << " prev " << res->first;
        sendMsg(ss.str(), pub);
        std::this_thread::sleep_for(std::chrono::duration(std::chrono::milliseconds(100)));
    }

    return res;
}

void pingAll(socket_t &pub, std::list<Node> &nodes) {
    std::vector<int> unavailableNodes;

    if (nodes.size() == 1){
        std::cout << "There are no Nodes" << "\n";
        return;
    }
    
    if (unavailableNodes.empty()) {
        std::cout << "Ok: -1" << "\n";
    } else {
        std::stringstream ss;
        ss << "Ok: ";
        for (size_t i = 0; i < unavailableNodes.size(); ++i) {
            ss << unavailableNodes[i];
            if (i < unavailableNodes.size() - 1) {
                ss << ", ";
            }
        }
        std::cout << ss.str() << "\n";
    }
}



int main() {
	std::list<Node> nodes{{-1, 0, 0, 0}};

    context_t ctx;
    socket_t pub(ctx, socket_type::push);
    socket_t sub(ctx, socket_type::pull);
    sub.connect(getAddrPrev(-1));
    std::string addr;
    int idMainCalcNode = -1;

    std::vector<std::thread> threads;

    while (true) {
        bool ok = true;
        print_mutex.lock();
        std::cout << "$ ";
        std::string s;
        std::cin >> s;
        print_mutex.unlock();

        if (s == "create") {
            int id;
            std::cin >> id;

            for (auto elem : nodes) {
                if (elem.id == id) {
                    std::cout << "Error: Already exists" << "\n";
                    ok = false;
                    break;
                }
            }
            int idParent = -2;
            bool isChild = false;
            getline(std::cin, s);
            if (s.size() > 0) {
                idParent = stoi(s);
                isChild = true;
            }

            if (!ok) continue;

            int idPrev, idNext;
            auto iterParent = nodes.end();
            if (isChild) {
                idPrev = idParent;

                auto i = nodes.begin();
                for (; i != nodes.end(); ++i)
                    if (i->id == idParent) {
                        iterParent = i;
                        ++i;
                        break;
                    }

                if (iterParent == nodes.end()) {
                    std::cout << "Error: Parent not found" << "\n";
                    continue;
                }
                
                if (i != nodes.end())
                    idNext = i->id;
                else
                    idNext = -2;
            } else {
                idNext = -2;
                idPrev = nodes.back().id;
            }

            pid_t process_id = create_process();
            if (process_id == 0) {
                std::this_thread::sleep_for(std::chrono::duration(std::chrono::milliseconds(1000)));
                execl("client.out", "client.out", itos(idPrev).c_str(), itos(id).c_str(), itos(idNext).c_str(), NULL);
                perror("exec");
                exit(-1);
            }
            
            Node node{id, isChild, idParent, process_id};
            if (isChild) {
                auto iterNode = nodes.insert(++iterParent, node);

                std::stringstream ss;
                if (idParent != -1) {
                    int idNodePrev = idParent;
                    ss << "rebind " << idNodePrev << " next " << id;
                    sendMsg(ss.str(), pub);
                    std::this_thread::sleep_for(std::chrono::duration(std::chrono::milliseconds(100)));
                }

                auto iterNodeNext = std::next(iterNode);
                if (iterNodeNext != nodes.end()) {
                    int idNodeNext = iterNodeNext->id;
                    ss.str("");
                    ss << "rebind " << idNodeNext << " prev " << id;
                    sendMsg(ss.str(), pub);
                    std::this_thread::sleep_for(std::chrono::duration(std::chrono::milliseconds(100)));
                }
            } else {
                nodes.push_back(node);
                
                auto iterNode = std::prev(nodes.end(), 2);
                int idNode = iterNode->id;
                if (idNode != -1) {
                    std::stringstream ss;
                    ss << "rebind " << idNode << " next " << id;
                    sendMsg(ss.str(), pub);
                    std::this_thread::sleep_for(std::chrono::duration(std::chrono::milliseconds(100)));
                }
            }

            if (idMainCalcNode == -1 || idParent == -1)
                changeMainCalcNode(pub, idMainCalcNode, id);

            print({"Ok: ", itos(process_id), "\n"});

            for (auto elem : nodes) {
                print_mutex.lock();
                std::cout << elem;
                print_mutex.unlock();
            }
        } else if (s == "kill") {
            int id;
            std::cin >> id;

            auto res = killNodeFull(id, nodes, idMainCalcNode, std::ref(pub), std::ref(sub), std::ref(threads));
            if (!res) {
                std::cout << "Error: Node not found" << "\n";
                continue;
            }

            for (auto elem : nodes) {
                print_mutex.lock();
                std::cout << elem;
                print_mutex.unlock();
            }
        } else if (s == "exec") {
            bool ok = true;

            int id, n;
            std::cin >> id >> n;

            auto i = nodes.begin();
            for (; i != nodes.end(); ++i)
                if (i->id == id)
                    break;
            if (i == nodes.end()) {
                std::cout << "Error: Node not found" << "\n";
                ok = false;
            }

            std::stringstream ss;
            ss << "exec " << id << " " << n << " ";
            for (int i = 0; i < n; ++i) {
                int a;
                std::cin >> a;
                ss << a << " ";
            }

            if (!ok) continue;

            threads.emplace_back(execThread, ss.str(), std::ref(pub), std::ref(sub), id);
        } else if (s == "pingall") {
            std::thread pingAllThread(pingAll, std::ref(pub), std::ref(nodes));
            pingAllThread.join();
        } else if (s == "exit") {
            for (auto elem : nodes) {
                kill_process(elem.pid);
            }
            break;
        } else if (s == "look") {
            std::this_thread::sleep_for(std::chrono::duration(std::chrono::milliseconds(10)));
        } else {
            std::cout << "Error: Unknown command" << "\n";
        }
    }

    for (auto &thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

	return 0;
}