/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef COMMON_HDFS_URI_H_
#define COMMON_HDFS_URI_H_

#include <iostream>
#include <string>
#include <optional.hpp>
#include <vector>

namespace hdfs
{

template <class T>
using optional = std::experimental::optional<T>;

class URI
{
    // These are stored in encoded form
    std::string scheme;
    std::string user;
    std::string pass;
    std::string host;
    optional<uint16_t> port;
    std::vector<std::string> path;
    std::vector<std::pair<std::string,std::string> > query;
    std::string fragment;

    template <class T>
    static T from_encoded(bool encoded_output, const T & input) {return encoded_output ? input : decode(input);}

    template <class T>
    static T to_encoded(bool encoded_input, const T & input) {return encoded_input ? input : encode(input);}

    bool has_authority() const;
    std::string build_authority(bool encoded_output) const;

    std::string build_path(bool encoded_output) const;
    void parse_path(bool input_encoded, const std::string &input_path);

public:
    // Parse a string into a URI.  Returns nullopt if the URI is malformed.
    static optional<URI> parse_from_string(const std::string &str);

    static std::string encode  (const std::string &input);
    static std::string decode  (const std::string &input);

    std::string get_scheme(bool encoded_output=false) const
    { return from_encoded(encoded_output,scheme); }

    void set_scheme(const std::string &s, bool encoded_input=false)
    { scheme = to_encoded(encoded_input,s); }

    // empty if none.
    std::string get_host(bool encoded_output=false) const
    { return from_encoded(encoded_output,host); }

    void set_host(const std::string& h, bool encoded_input=false)
    { host = to_encoded(encoded_input,h); }

    // -1 if the port is undefined.
    optional<uint16_t> get_port() const
    { return port; }

    void set_port(uint16_t p)
    { port = p; }

    void clear_port()
    { port = std::experimental::nullopt; }

    std::string get_path(bool encoded_output=false) const;

    std::vector<std::string> get_path_elements(bool encoded_output=false) const;

    void set_path(const std::string &p, bool encoded_input=false) {
        parse_path(encoded_input, p);
    }

    void add_path(const std::string &p, bool encoded_input=false);

    std::string get_query(bool encoded_output=false) const;

    std::vector< std::pair<std::string, std::string> > get_query_elements(bool encoded_output=false) const;

    // Not that set_query must always pass in encoded strings
    void set_query(const std::string &q);

    // Adds a parameter onto the query; does not check if it already exists
    //   e.g. parseFromString("foo?bar=baz").addQuery("bing","bang")
    //   would leave "bar=baz&bing=bang" as the query
    void add_query(const std::string &name, const std::string & value, bool encoded_input=false);

    // Removes the query part if exists
    //   e.g. parseFromString("foo?bar=baz&bing=bang&bar=bong").removeQueries("bar")
    //   would leave bing=bang as the query
    void remove_queries(const std::string &q_name, bool encoded_input=false);

    std::string get_fragment(bool encoded_output=false) const
    { return from_encoded(encoded_output, fragment); }

    void set_fragment(const std::string &f, bool encoded_input=false)
    { fragment = to_encoded(encoded_input,f); }

    std::string str(bool encoded_output=true) const;
};

inline std::ostream& operator<<(std::ostream &out, const URI &uri)
{ return out << uri.str(); }

}

#endif