#include <iostream>
#include <zmq.hpp>
#include <thread>
#include "lib.hpp"

using namespace zmq;

const int MAX_LEN_MSG = 256;

void waitResponse(socket_t &pubPrev, socket_t &subResponse) {
    message_t response(MAX_LEN_MSG);
    auto res_r = subResponse.recv(response, recv_flags::none);
    if (!res_r.has_value()) {
        std::cout << "Get error" << "\n";
        return;
    }
    auto res_s = pubPrev.send(response, send_flags::none);
    if (!res_s.has_value()) {
        std::cout << "Get error" << "\n";
        return;
    }
}

void unbindPubPrev(socket_t &pub, int id) {
	pub.unbind(getAddrPrev(id));
}
void unbindPubNext(socket_t &pub, int id) {
	pub.unbind(getAddrNext(id));
}

void unbindPubs(socket_t &pubPrev, socket_t &pubNext, int idPrev, int idNext) {
	unbindPubPrev(pubPrev, idPrev);
	if (idNext != -2)
		unbindPubNext(pubNext, idNext);
}

void bindPubPrev(socket_t &pub, int id) {
	pub.bind(getAddrPrev(id));
}
void bindPubNext(socket_t &pub, int id) {
	pub.bind(getAddrNext(id));
}

void bindPubs(socket_t &pubPrev, socket_t &pubNext, int idPrev, int idNext) {
	bindPubPrev(pubPrev, idPrev);
	if (idNext != -2)
		bindPubNext(pubNext, idNext);
}

void rebindPubPrev(socket_t &pub, int idOld, int id) {
	pub.unbind(getAddrPrev(idOld));
	bindPubPrev(std::ref(pub), id);
}
void rebindPubNext(socket_t &pub, int idOld, int id) {
	if (idOld != -2)
		pub.unbind(getAddrNext(idOld));
	bindPubNext(std::ref(pub), id);
}

void unbindSockets(socket_t &pubPrev, socket_t &pubNext, socket_t &subRequest, socket_t &subResponse, socket_t &pubHeartbit, int idPrev, int id, int idNext) {
	unbindPubs(std::ref(pubPrev), std::ref(pubNext), idPrev, idNext);
	subRequest.disconnect(getAddr( getPort(id) ));
	subResponse.disconnect(getAddr( getPort(id) + 1 ));
	pubHeartbit.disconnect(getAddrNext(-1));
}

void unbind(socket_t &pubPrev, socket_t &pubNext, socket_t &subRequest, socket_t &subResponse, socket_t &pubHeartbit, int idPrev, int id, int idNext, bool immediately, std::string msg) {
	std::stringstream ss;
	bool isKill = false;
	if (immediately) {
		ss.str(msg);
		isKill = true;
	} else {
		message_t response(MAX_LEN_MSG);
		auto res_r = subResponse.recv(response, recv_flags::none);
		if (!res_r.has_value()) {
			std::cout << "Get error" << "\n";
			return;
		}
		ss.str(response.to_string());
		std::string mode, submode; int id1, id2;
		ss >> mode >> id1 >> id2 >> submode;
		ss.str("");
		ss.clear();
		if (submode == "active") {
			if (id != id1) {
				isKill = true;
			}
		}
	}

    auto res_s = pubPrev.send(buffer(ss.str()), send_flags::none);
    if (!res_s.has_value()) {
        std::cout << "Get error" << "\n";
        return;
    }

	if (isKill) {
		unbindSockets(std::ref(pubPrev), std::ref(pubNext), std::ref(subRequest), std::ref(subResponse), std::ref(pubHeartbit), idPrev, id, idNext);
		exit(EXIT_SUCCESS);
	}
}

int main(int argc, char* argv[]) {
	assert(argc == 4);

	int idPrev = std::stoi(argv[1]);
	int id = std::stoi(argv[2]);
	int idNext = std::stoi(argv[3]);

	context_t ctx;
    socket_t pubPrev(ctx, socket_type::push);
    socket_t pubNext(ctx, socket_type::push);
    socket_t subRequest(ctx, socket_type::pull);
    socket_t subResponse(ctx, socket_type::pull);

	bindPubs(std::ref(pubPrev), std::ref(pubNext), idPrev, idNext);
	subRequest.connect(getAddr( getPort(id) ));
	subResponse.connect(getAddr( getPort(id) + 1 ));

	std::vector<std::thread> threads;
	socket_t pubHeartbit(ctx, socket_type::push);
	pubHeartbit.connect(getAddrNext(-1));
	std::optional<std::thread> threadHeartbit;

	while (true) {
		message_t request(MAX_LEN_MSG);
		auto res_r = subRequest.recv(request, recv_flags::none);
		if (!res_r.has_value()) {
			std::cout << "Get error" << "\n";
			break;
		}
		std::stringstream ss(request.to_string());
		std::string mode;
		ss >> mode;

		if (mode == "exec") {
			int idTarget, n;
			ss >> idTarget >> n;
			
			if (id == idTarget) {
				std::string res = "";
				int sum_ = 0;
				int x;
				for (int i = 0; i < n; ++i) {
					ss >> x;
					sum_ += x;
				}

				res = itos(sum_);

				mutable_buffer mbuf = buffer(res);
				auto res_s = pubPrev.send(mbuf, send_flags::none);
				if (!res_s.has_value()) {
					std::cout << "Send error" << "\n";
					break;
				}
			} else {
				auto res_s = pubNext.send(request, send_flags::none);
				if (!res_s.has_value()) {
					std::cout << "Send error" << "\n";
					break;
				}

				threads.emplace_back(waitResponse, std::ref(pubPrev), std::ref(subResponse));
			}
		} else if (mode == "rebind") {
			int idTarget;
			ss >> idTarget;

			if (id == idTarget) {
				std::string submode;
				ss >> submode;
				
				int idNewPrev, idNewNext;
				if (submode == "prev") {
					ss >> idNewPrev;
					rebindPubPrev(std::ref(pubPrev), idPrev, idNewPrev);
					idPrev = idNewPrev;
				}
				if (submode == "next") {
					ss >> idNewNext;
					rebindPubNext(std::ref(pubNext), idNext, idNewNext);
					idNext = idNewNext;
				}
			} else {

				auto res_s = pubNext.send(request, send_flags::none);
				if (!res_s.has_value()) {
					std::cout << "Send error" << "\n";
					break;
				}
			}
		} else if (mode == "unbind") {
			int id1, id2;
			ss >> id1 >> id2;

			std::string submode;
			ss >> submode;

			std::stringstream ssMsg;
			ssMsg << "unbind " << id1 << " " << id2;

			if (submode == "search") {
				std::string who;
				ss >> who;
				if (who == "first") {
					ssMsg << " search";
					if (id == id1) {
						ssMsg << " last";
					} else {
						ssMsg << " first";
					}
				} else if (who == "last") {
					if (id == id2) {
						ssMsg << " active";
		
						auto res_s = pubPrev.send(buffer(ssMsg.str()), send_flags::none);
						if (!res_s.has_value()) {
							std::cout << "Send error" << "\n";
							break;
						}

						continue;
					} else if (idNext == -2) {
						ssMsg << " active";
						threads.emplace_back(unbind, std::ref(pubPrev), std::ref(pubNext), std::ref(subRequest), std::ref(subResponse), std::ref(pubHeartbit), idPrev, id, idNext, true, ssMsg.str());
						continue;
					} else {
						ssMsg << " search last";
					}
				}
				auto res_s = pubNext.send(buffer(ssMsg.str()), send_flags::none);
				if (!res_s.has_value()) {
					std::cout << "Send error" << "\n";
					break;;
				}
				threads.emplace_back(unbind, std::ref(pubPrev), std::ref(pubNext), std::ref(subRequest), std::ref(subResponse), std::ref(pubHeartbit), idPrev, id, idNext, false, "");
			} else {
				break;
			}
		} else {
			break;
		}
	}
	
	return 0;
}