
#include "mimalloc-new-delete.h" //

// only 64bit..
#include "simdclaujson.h"


class Parse {
public:

	simdjson::dom::parser* parser;
	void operator()(bool x, int64_t y, int64_t z, int no, simdjson::dom::element* output)
	{
		try {
			*output = parser->parse(x, y, z, no);
		}
		catch (simdjson::simdjson_error e) {
			std::cout << e.what() << "\n";
			exit(-1);
		}
	}

};

int main(int argc, char* argv[])
{
	int a, b, c;
	int start = clock();

	clau::UserType global;

	{
		simdjson::dom::parser parser;

		size_t len = 0;
		a = clock();

		
		parser.docs = std::vector<simdjson::dom::document>(8);
		for (int i = 0; i < 8; ++i) {
			parser.docs[i].ori_doc = &parser.doc;
			parser.docs[i].state = 1;
			parser.docs[i].no = i;
		}

		auto tweets2 = parser.load(argv[1], false, 0);

		if (tweets2.error() != simdjson::error_code::SUCCESS) {
			std::cout << tweets2.error() << " ";
			return 1;
		}


		len = parser.len();
		auto split = parser.split();

		b = clock();

		std::cout << b - a << "ms\n";


		//std::cout << "len " << len << "\n";

		a = clock();

		auto count = parser.count();
		std::vector<int64_t> start(8, 0);

		for (int i = 0; i < start.size(); ++i) {
			start[i] = split[i];
		}

		std::vector<int64_t> length(start.size());

		length[start.size() - 1] = len - start[start.size() - 1];

		for (int i = 0; i < start.size() - 1; ++i) {
			length[i] = start[i + 1] - start[i]; //  + 1] - len2[i];

			//std::cout << "length " << length[i] << " ";
		}

		size_t sum = 0;
		for (int i = length.size() - 1; i >= 0; --i) {
			if (length[i] > 0) {
				sum += length[i];
			}
		}

		//std::cout << sum << " " << len << "\n";

		std::vector<const simdjson::Token*> token_arr(8);
		std::vector<size_t> Len(8, 0);

		std::vector<simdjson::dom::element> tweets(8);
		std::vector<int> chk(8, false);

		try {
			std::thread* thr[8] = { nullptr };

			for (int i = 0; i < 8; ++i) {
				Parse temp;
				temp.parser = &parser;

				if (length[i] > 0) {
					thr[i] = new std::thread(temp, true, length[i], start[i], i, &tweets[i]);
					chk[i] = true;
				}
				else {
					thr[i] = nullptr;
				}
			}

			for (int i = 0; i < 8; ++i) {
				if (chk[i]) {
					thr[i]->join();
				}
			}

			for (int i = 0; i < 8; ++i) {
				if (chk[i]) {
					delete thr[i];
				}
			}

			for (int i = 0; i < 8; ++i) {
				if (chk[i]) {
					token_arr[i] = (tweets[i].raw_tape().get());
					Len[i] = (parser.len(i));
				}
			//	std::cout << "start " << start[i] << " ";
			//	std::cout << length[i] << " " << Len[i] << "\n";
			}
		}
		catch (simdjson::simdjson_error e) {
			std::cout << e.what() << "\n";
		}


		b = clock();
		std::cout << len << "\n";



		std::cout << b - a << "ms\n";


		a = clock();

		{
			/*
			auto& stream = std::cout;
			
			for (int i = 0; i < token_arr.size(); ++i) {
				for (int j = 0; j < Len[i]; ++j) {
					if (token_arr[i][j].get_type() == simdjson::internal::tape_type::TRUE_VALUE) {
						stream << "true";
					}
					else if (token_arr[i][j].get_type() == simdjson::internal::tape_type::FALSE_VALUE) {
						stream << "false";
					}
					else if (token_arr[i][j].get_type() == simdjson::internal::tape_type::DOUBLE) {
						stream << (token_arr[i][j].data.float_val);
					}
					else if (token_arr[i][j].get_type() == simdjson::internal::tape_type::INT64) {
						stream << (token_arr[i][j].data.int_val);
					}
					else if (token_arr[i][j].get_type() == simdjson::internal::tape_type::UINT64) {
						stream << (token_arr[i][j].data.uint_val);
					}
					else if (token_arr[i][j].get_type() == simdjson::internal::tape_type::NULL_VALUE) {
						stream << "null ";
					}
					else if (token_arr[i][j].is_key()) {
						stream << "." << j << " \"" << (token_arr[i][j].get_str()) << "\"";
					}
					else if (token_arr[i][j].get_type() == simdjson::internal::tape_type::STRING) {
						stream << ".." << j << " \"" << (token_arr[i][j].get_str()) << "\"";
					}
					else {
						stream << "chk " << j << " " << (int)token_arr[i][j].get_type();
					}

					std::cout << " ";
				}std::cout << "\n";
				
			}*/

			
			clau::LoadData::parse(token_arr, Len, global, 8);
		}

		c = clock();
	}
	b = clock();
	
	std::cout << c - start << "ms\n";

	std::cout << b - start << "ms\n";

	//clau::LoadData::save("output.txt2", global); // debug..
	//clau::LoadData::_save(std::cout, &global);

	return 0;
}

