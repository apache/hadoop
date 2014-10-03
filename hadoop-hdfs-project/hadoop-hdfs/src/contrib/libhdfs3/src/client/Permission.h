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
#ifndef _HDFS_LIBHDFS3_CLIENT_PERMISSION_H_
#define _HDFS_LIBHDFS3_CLIENT_PERMISSION_H_

#include <string>

namespace hdfs {

/**
 * Action is used to describe a action the user is permitted to apply on a file.
 */
enum Action {
    NONE, //("---"),
    EXECUTE, //("--x"),
    WRITE, //("-w-"),
    WRITE_EXECUTE, //("-wx"),
    READ, //("r--"),
    READ_EXECUTE, //("r-x"),
    READ_WRITE, //("rw-"),
    ALL //("rwx");
};

/**
 * To test Action a if implies Action b
 * @param a Action to be tested.
 * @param b Action target.
 * @return return true if a implies b.
 */
static inline bool implies(const Action &a, const Action &b) {
    return (a & b) == b;
}

/**
 * To construct a new Action using a and b
 * @param a Action to be used.
 * @param b Action to be used.
 * @return return a new Action.
 */
static inline Action operator &(const Action &a, const Action &b) {
    return (Action)(((unsigned int) a) & (unsigned int) b);
}
/**
 * To construct a new Action using a or b
 * @param a Action to be used.
 * @param b Action to be used.
 * @return return a new Action.
 */
static inline Action operator |(const Action &a, const Action &b) {
    return (Action)(((unsigned int) a) | (unsigned int) b);
}
/**
 * To construct a new Action of complementary of a given Action
 * @param a Action to be used.
 * @return return a new Action
 */
static inline Action operator ~(const Action &a) {
    return (Action)(7 - (unsigned int) a);
}

/**
 * To convert a Action to a readable string.
 * @param a the Action to be convert.
 * @return a readable string
 */
static inline std::string toString(const Action &a) {
    switch (a) {
    case NONE:
        return "---";

    case EXECUTE:
        return "--x";

    case WRITE:
        return "-w-";

    case WRITE_EXECUTE:
        return "-wx";

    case READ:
        return "r--";

    case READ_EXECUTE:
        return "r-x";

    case READ_WRITE:
        return "rw-";

    case ALL:
        return "rwx";
    }
}

/**
 * Permission is used to describe a file permission.
 */
class Permission {
public:
    /**
     * To construct a Permission.
     * @param u owner permission.
     * @param g group permission.
     * @param o other user permission.
     */
    Permission(const Action &u, const Action &g, const Action &o) :
        userAction(u), groupAction(g), otherAction(o), stickyBit(false) {
    }

    /**
     * To construct a Permission from a uint16.
     * @param mode permission flag.
     */
    Permission(uint16_t mode);

public:
    /**
     * To get group permission
     * @return the group permission
     */
    Action getGroupAction() const {
        return groupAction;
    }

    /**
     * To set group permission
     * @param groupAction the group permission
     */
    void setGroupAction(Action groupAction) {
        this->groupAction = groupAction;
    }

    /**
     * To get other user permission
     * @return other user permission
     */
    Action getOtherAction() const {
        return otherAction;
    }

    /**
     * To set other user permission
     * @param otherAction other user permission
     */
    void setOtherAction(Action otherAction) {
        this->otherAction = otherAction;
    }

    /**
     * To get owner permission
     * @return the owner permission
     */
    Action getUserAction() const {
        return userAction;
    }

    /**
     * To set owner permission
     * @param userAction the owner permission
     */
    void setUserAction(Action userAction) {
        this->userAction = userAction;
    }

    /**
     * To convert a Permission to a readable string
     * @return a readable string
     */
    std::string toString() const {
        return hdfs::toString(userAction) + hdfs::toString(groupAction)
               + hdfs::toString(otherAction);
    }

    /**
     * To convert a Permission to a uint16 flag
     * @return a uint16 flag
     */
    uint16_t toShort() const {
        return (uint16_t)((((uint16_t) userAction) << 6)
                          + (((uint16_t) groupAction) << 3) + (((uint16_t) otherAction))
                          + ((stickyBit ? 1 << 9 : 0)));
    }

    bool operator ==(const Permission &other) const {
        return userAction == other.userAction
               && groupAction == other.groupAction
               && otherAction == other.otherAction
               && stickyBit == other.stickyBit;
    }

private:
    Action userAction;
    Action groupAction;
    Action otherAction;

    bool stickyBit;
};

}

#endif
