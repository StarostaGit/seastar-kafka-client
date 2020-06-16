#ifndef __STREAMS_HH__
#define __STREAMS_HH__

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <vector>
#include <cstring>

using std::vector;

namespace kafka4seastar {
    struct parsing_exception : public std::runtime_error {
    public:
        parsing_exception(const std::string& message) : runtime_error(message) {}
    };

    namespace kafka {

    class input_stream {
    private:
        const char *_data;
        int32_t _length;
        int32_t _current_position = 0;
        int32_t _gcount = 0;

    public:
        input_stream(const char *data, int32_t length) noexcept
            : _data(data)
            , _length(length)
        {}

        inline void read(char *destination, int32_t length) {
            if(_current_position + length > _length){
                throw kafka4seastar::parsing_exception ("Attempted to read more than the remaining length of the input stream.");
            }
            std::copy((_data + _current_position), (_data + _current_position + length), destination);
            _current_position += length;
            _gcount = length;
        }

        inline int32_t gcount() const {
            return _gcount;
        }

        inline const char *get() const {
            if(_current_position == _length) {
                throw kafka4seastar::parsing_exception("Input stream is exhausted.");
            }
            return _data + _current_position;
        }

        inline int32_t get_position() const noexcept {
            return _current_position;
        }

        inline void set_position(int32_t new_position) {
            if(new_position >= _length || new_position < 0) {
                throw kafka4seastar::parsing_exception("Attempted to set input stream's position outside its data.");
            }
            else {
                _current_position = new_position;
            }
        }

        inline void move_position(int32_t delta) {
            set_position(_current_position + delta);
        }

        int32_t size() const noexcept {
            return _length;
        }

        inline const char * begin() const {
            return _data;
        }
    };

    class output_stream {
    private:
        char* _data;
        int _size;
        int _capacity; 

        int32_t _current_position = 0;
        bool _is_resizable;

        output_stream(bool is_resizable, int32_t size) noexcept
                :_is_resizable(is_resizable),
                  _capacity(size),
                  _data((char*)malloc(size+5)),
                 _size(size)
        {}

    public:

	~output_stream() { free(_data); }

        static output_stream fixed_size_stream(int32_t size) {
            return output_stream(false, size);
        }

        static output_stream resizable_stream() {
            return output_stream(true, 0);
        }

        inline void write(const char* source, int32_t length) {
            if (length + _current_position > _size) {
                if (_is_resizable) {
                    if (length + _current_position > _capacity) {
                    _capacity = std::max(_capacity * 2, length + _current_position);
                    _data = (char*)realloc(_data, _capacity); }
                    _size = length + _current_position ;
                } else {
                    throw kafka4seastar::parsing_exception("This output stream won't that many bytes");
                }
            }
            memcpy(_data + _current_position, source, length);
            _current_position += length;
        }


        inline char *get() {
            if(_current_position == _size) {
                throw kafka4seastar::parsing_exception("Output stream is full.");
            }
            return _data + _current_position;
        }

        inline const char *get() const {
            if(_current_position == static_cast<int32_t>(_size)) {
                throw kafka4seastar::parsing_exception("Output stream is full.");
            }
            return _data + _current_position;
        }

        inline const char * begin() const {
            return _data;
        }

        inline int32_t get_position() const noexcept {
            return _current_position;
        }

        inline void set_position(int32_t new_position) {
            if(new_position < 0) {
                throw kafka4seastar::parsing_exception("Cannot set position to negative value.");
            }

            if(_is_resizable) {
                if (new_position >= _size) {
		   if (new_position + 1 > _capacity) {
                    _capacity = std::max(_capacity * 2, new_position + 1);
                    _data = (char*)realloc(_data, _capacity);}
                    _size  = new_position + 1;

                }
            }
            else {
                if(new_position >= static_cast<int32_t>(_size)) {
                    kafka4seastar::parsing_exception("Attempted to set fixed output stream's position past its data.");
                }
            }

            _current_position = new_position;
        }

        inline void move_position(int32_t delta) {
            set_position(_current_position + delta);
        }

        int32_t size() const noexcept {
            return _size;
        }

        void reset() noexcept {
            _current_position = 0;
            _size = 0;
        }
    };

} // namespace kafka

} // namespace seastar

#endif // __STREAMS_HH__
