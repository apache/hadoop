
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.processing.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "type",
    "matches",
    "policy",
    "parentQueue",
    "fallbackResult",
    "create",
    "value",
    "customPlacement"
})
@Generated("jsonschema2pojo")
public class Rule {

    @JsonProperty("type")
    private Rule.Type type;
    @JsonProperty("matches")
    private String matches;
    @JsonProperty("policy")
    private Rule.Policy policy;
    @JsonProperty("parentQueue")
    private String parentQueue;
    @JsonProperty("fallbackResult")
    private Rule.FallbackResult fallbackResult;
    @JsonProperty("create")
    private Boolean create;
    @JsonProperty("value")
    private String value;
    @JsonProperty("customPlacement")
    private String customPlacement;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("type")
    public Rule.Type getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(Rule.Type type) {
        this.type = type;
    }

    @JsonProperty("matches")
    public String getMatches() {
        return matches;
    }

    @JsonProperty("matches")
    public void setMatches(String matches) {
        this.matches = matches;
    }

    @JsonProperty("policy")
    public Rule.Policy getPolicy() {
        return policy;
    }

    @JsonProperty("policy")
    public void setPolicy(Rule.Policy policy) {
        this.policy = policy;
    }

    @JsonProperty("parentQueue")
    public String getParentQueue() {
        return parentQueue;
    }

    @JsonProperty("parentQueue")
    public void setParentQueue(String parentQueue) {
        this.parentQueue = parentQueue;
    }

    @JsonProperty("fallbackResult")
    public Rule.FallbackResult getFallbackResult() {
        return fallbackResult;
    }

    @JsonProperty("fallbackResult")
    public void setFallbackResult(Rule.FallbackResult fallbackResult) {
        this.fallbackResult = fallbackResult;
    }

    @JsonProperty("create")
    public Boolean getCreate() {
        return create;
    }

    @JsonProperty("create")
    public void setCreate(Boolean create) {
        this.create = create;
    }

    @JsonProperty("value")
    public String getValue() {
        return value;
    }

    @JsonProperty("value")
    public void setValue(String value) {
        this.value = value;
    }

    @JsonProperty("customPlacement")
    public String getCustomPlacement() {
        return customPlacement;
    }

    @JsonProperty("customPlacement")
    public void setCustomPlacement(String customPlacement) {
        this.customPlacement = customPlacement;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(Rule.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("type");
        sb.append('=');
        sb.append(((this.type == null)?"<null>":this.type));
        sb.append(',');
        sb.append("matches");
        sb.append('=');
        sb.append(((this.matches == null)?"<null>":this.matches));
        sb.append(',');
        sb.append("policy");
        sb.append('=');
        sb.append(((this.policy == null)?"<null>":this.policy));
        sb.append(',');
        sb.append("parentQueue");
        sb.append('=');
        sb.append(((this.parentQueue == null)?"<null>":this.parentQueue));
        sb.append(',');
        sb.append("fallbackResult");
        sb.append('=');
        sb.append(((this.fallbackResult == null)?"<null>":this.fallbackResult));
        sb.append(',');
        sb.append("create");
        sb.append('=');
        sb.append(((this.create == null)?"<null>":this.create));
        sb.append(',');
        sb.append("value");
        sb.append('=');
        sb.append(((this.value == null)?"<null>":this.value));
        sb.append(',');
        sb.append("customPlacement");
        sb.append('=');
        sb.append(((this.customPlacement == null)?"<null>":this.customPlacement));
        sb.append(',');
        sb.append("additionalProperties");
        sb.append('=');
        sb.append(((this.additionalProperties == null)?"<null>":this.additionalProperties));
        sb.append(',');
        if (sb.charAt((sb.length()- 1)) == ',') {
            sb.setCharAt((sb.length()- 1), ']');
        } else {
            sb.append(']');
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = ((result* 31)+((this.fallbackResult == null)? 0 :this.fallbackResult.hashCode()));
        result = ((result* 31)+((this.customPlacement == null)? 0 :this.customPlacement.hashCode()));
        result = ((result* 31)+((this.create == null)? 0 :this.create.hashCode()));
        result = ((result* 31)+((this.parentQueue == null)? 0 :this.parentQueue.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.type == null)? 0 :this.type.hashCode()));
        result = ((result* 31)+((this.matches == null)? 0 :this.matches.hashCode()));
        result = ((result* 31)+((this.value == null)? 0 :this.value.hashCode()));
        result = ((result* 31)+((this.policy == null)? 0 :this.policy.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof Rule) == false) {
            return false;
        }
        Rule rhs = ((Rule) other);
        return ((((((((((this.fallbackResult == rhs.fallbackResult)||((this.fallbackResult!= null)&&this.fallbackResult.equals(rhs.fallbackResult)))&&((this.customPlacement == rhs.customPlacement)||((this.customPlacement!= null)&&this.customPlacement.equals(rhs.customPlacement))))&&((this.create == rhs.create)||((this.create!= null)&&this.create.equals(rhs.create))))&&((this.parentQueue == rhs.parentQueue)||((this.parentQueue!= null)&&this.parentQueue.equals(rhs.parentQueue))))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))))&&((this.type == rhs.type)||((this.type!= null)&&this.type.equals(rhs.type))))&&((this.matches == rhs.matches)||((this.matches!= null)&&this.matches.equals(rhs.matches))))&&((this.value == rhs.value)||((this.value!= null)&&this.value.equals(rhs.value))))&&((this.policy == rhs.policy)||((this.policy!= null)&&this.policy.equals(rhs.policy))));
    }

    @Generated("jsonschema2pojo")
    public enum FallbackResult {

        SKIP("skip"),
        REJECT("reject"),
        PLACE_DEFAULT("placeDefault");
        private final String value;
        private final static Map<String, Rule.FallbackResult> CONSTANTS = new HashMap<String, Rule.FallbackResult>();

        static {
            for (Rule.FallbackResult c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        FallbackResult(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        @JsonValue
        public String value() {
            return this.value;
        }

        @JsonCreator
        public static Rule.FallbackResult fromValue(String value) {
            Rule.FallbackResult constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    @Generated("jsonschema2pojo")
    public enum Policy {

        SPECIFIED("specified"),
        REJECT("reject"),
        DEFAULT_QUEUE("defaultQueue"),
        USER("user"),
        PRIMARY_GROUP("primaryGroup"),
        SECONDARY_GROUP("secondaryGroup"),
        PRIMARY_GROUP_USER("primaryGroupUser"),
        SECONDARY_GROUP_USER("secondaryGroupUser"),
        APPLICATION_NAME("applicationName"),
        SET_DEFAULT_QUEUE("setDefaultQueue"),
        CUSTOM("custom");
        private final String value;
        private final static Map<String, Rule.Policy> CONSTANTS = new HashMap<String, Rule.Policy>();

        static {
            for (Rule.Policy c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Policy(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        @JsonValue
        public String value() {
            return this.value;
        }

        @JsonCreator
        public static Rule.Policy fromValue(String value) {
            Rule.Policy constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    @Generated("jsonschema2pojo")
    public enum Type {

        USER("user"),
        GROUP("group"),
        APPLICATION("application");
        private final String value;
        private final static Map<String, Rule.Type> CONSTANTS = new HashMap<String, Rule.Type>();

        static {
            for (Rule.Type c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Type(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        @JsonValue
        public String value() {
            return this.value;
        }

        @JsonCreator
        public static Rule.Type fromValue(String value) {
            Rule.Type constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
