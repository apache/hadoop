//
//  ========================================================================
//  Copyright (c) 1995-2022 Mort Bay Consulting Pty Ltd and others.
//  ------------------------------------------------------------------------
//  All rights reserved. This program and the accompanying materials
//  are made available under the terms of the Eclipse Public License v1.0
//  and Apache License v2.0 which accompanies this distribution.
//
//      The Eclipse Public License is available at
//      http://www.eclipse.org/legal/epl-v10.html
//
//      The Apache License v2.0 is available at
//      http://www.opensource.org/licenses/apache2.0.php
//
//  You may elect to redistribute this code under either of these licenses.
//  ========================================================================
//

package org.apache.hadoop.hdfs.server.federation.metrics;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * <b>Internal Use Only</b> - a cutdown copy of the JSON class from jetty-util-ajax 9.4.
 * <p>
 * JSON Generator only. Do not use this for new code. It is kept for a small number
 * of legacy use cases.
 * <p>
 * This class provides some static methods to convert POJOs to JSON
 * notation. The java to JSON mapping is:
 *
 * <pre>
 *   String --&gt; string
 *   Number --&gt; number
 *   Map    --&gt; object
 *   List   --&gt; array
 *   Array  --&gt; array
 *   null   --&gt; null
 *   Boolean--&gt; boolean
 *   Object --&gt; string (dubious!)
 * </pre>
 */
public class JSON {
    private static final JSON DEFAULT = new JSON();
    private static final int _stringBufferSize = 1024;

    public JSON() {
    }

    public static JSON getDefault() {
        return DEFAULT;
    }

    /**
     * Convert Object to JSON
     *
     * @param object The object to convert
     * @return The JSON String
     */
    public String toJSON(Object object) {
        StringBuilder buffer = new StringBuilder(getStringBufferSize());
        append(buffer, object);
        return buffer.toString();
    }

    /**
     * @return the initial stringBuffer size to use when creating JSON strings
     * (default 1024)
     */
    private int getStringBufferSize() {
        return _stringBufferSize;
    }

    private void quotedEscape(Appendable buffer, String input) {
        try {
            buffer.append('"');
            if (input != null && !input.isEmpty()) {
                escapeString(buffer, input);
            }
            buffer.append('"');
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void escapeString(Appendable buffer, String input) throws IOException {
        // default escaping here.

        for (int i = 0; i < input.length(); ++i) {
            char c = input.charAt(i);

            // ASCII printable range
            if ((c >= 0x20) && (c <= 0x7E)) {
                // Special cases for quotation-mark, reverse-solidus, and solidus
                if ((c == '"') || (c == '\\')
                  /* solidus is optional - per Carsten Bormann (IETF)
                     || (c == '/') */) {
                    buffer.append('\\').append(c);
                } else {
                    // ASCII printable (that isn't escaped above)
                    buffer.append(c);
                }
            } else {
                // All other characters are escaped (in some way)

                // First we deal with the special short-form escaping
                if (c == '\b') // backspace
                    buffer.append("\\b");
                else if (c == '\f') // form-feed
                    buffer.append("\\f");
                else if (c == '\n') // line feed
                    buffer.append("\\n");
                else if (c == '\r') // carriage return
                    buffer.append("\\r");
                else if (c == '\t') // tab
                    buffer.append("\\t");
                else if (c < 0x20 || c == 0x7F) // all control characters
                {
                    // default behavior is to encode
                    buffer.append(String.format("\\u%04x", (short) c));
                } else {
                    // optional behavior in JSON spec
                    escapeUnicode(buffer, c);
                }
            }
        }
    }

    /**
     * Per spec, unicode characters are by default NOT escaped.
     */
    private void escapeUnicode(Appendable buffer, char c) throws IOException {
        buffer.append(c);
    }

    /**
     * Append object as JSON to string buffer.
     *
     * @param buffer the buffer to append to
     * @param object the object to append
     */
    private void append(Appendable buffer, Object object) {
        try {
            if (object == null) {
                buffer.append("null");
            }
            // Most likely first
            else if (object instanceof Map) {
                appendMap(buffer, (Map) object);
            } else if (object instanceof String) {
                appendString(buffer, (String) object);
            } else if (object instanceof Number) {
                appendNumber(buffer, (Number) object);
            } else if (object instanceof Boolean) {
                appendBoolean(buffer, (Boolean) object);
            } else if (object.getClass().isArray()) {
                appendArray(buffer, object);
            } else if (object instanceof Character) {
                appendString(buffer, object.toString());
            } else if (object instanceof Collection) {
                appendArray(buffer, (Collection) object);
            } else {
                appendString(buffer, object.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void appendNull(Appendable buffer) {
        try {
            buffer.append("null");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void appendMap(Appendable buffer, Map<?, ?> map) {
        try {
            if (map == null) {
                appendNull(buffer);
                return;
            }

            buffer.append('{');
            Iterator<?> iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) iter.next();
                quotedEscape(buffer, entry.getKey().toString());
                buffer.append(':');
                append(buffer, entry.getValue());
                if (iter.hasNext())
                    buffer.append(',');
            }

            buffer.append('}');
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void appendArray(Appendable buffer, Collection collection) {
        try {
            if (collection == null) {
                appendNull(buffer);
                return;
            }

            buffer.append('[');
            Iterator iter = collection.iterator();
            boolean first = true;
            while (iter.hasNext()) {
                if (!first)
                    buffer.append(',');

                first = false;
                append(buffer, iter.next());
            }

            buffer.append(']');
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void appendArray(Appendable buffer, Object array) {
        try {
            if (array == null) {
                appendNull(buffer);
                return;
            }

            buffer.append('[');
            int length = Array.getLength(array);

            for (int i = 0; i < length; i++) {
                if (i != 0)
                    buffer.append(',');
                append(buffer, Array.get(array, i));
            }

            buffer.append(']');
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void appendBoolean(Appendable buffer, Boolean b) {
        try {
            if (b == null) {
                appendNull(buffer);
                return;
            }
            buffer.append(b ? "true" : "false");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void appendNumber(Appendable buffer, Number number) {
        try {
            if (number == null) {
                appendNull(buffer);
                return;
            }
            buffer.append(String.valueOf(number));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void appendString(Appendable buffer, String string) {
        if (string == null) {
            appendNull(buffer);
            return;
        }

        quotedEscape(buffer, string);
    }
}
