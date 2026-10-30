package io.hydrangea.codec;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Records to and from the plain-object form every document format here decodes to ({@code Map}, {@code List}, strings, numbers, booleans, null, and a format's own
 * scalars such as dates or ids), by reflection on the record components: no library, no code generation. A document client wraps one of these with its own
 * name annotation ({@code @Json.Name}, {@code @Bson.Name}) for a JSON/BSON field named differently from the component.
 * <ul>
 *   <li>{@link #to} builds a record from a map: a component may be a primitive or its box, {@code String}, an enum (by name, case-insensitively as a fallback),
 *       another record, {@code List}/{@code Set}/{@code Collection}/{@code Map} with their type arguments, an array, {@code Optional}, {@code Object} (left as is),
 *       {@code BigDecimal}/{@code BigInteger}, {@code UUID}, {@code byte[]} (as is, or from base64 text), the {@code java.time} types (as is, or parsed from ISO text;
 *       {@code Instant} also from epoch milliseconds), or any type the value already is. A field missing from the map gives null (0 or false for a primitive);
 *       a field the record has no component for is ignored. A value of the wrong shape fails with the path to it ({@code at items[2].price: ...}).</li>
 *   <li>{@link #fields} gives a record's components as an ordered map, nested values untouched: a writer recurses into them as it does into any value.</li>
 * </ul>
 */
public final class Records {

    private final Class<? extends Annotation> nameAnnotation;
    private final ClassValue<Shape> shapes = new ClassValue<>() {
        @Override
        protected Shape computeValue(Class<?> type) {
            return new Shape(type);
        }
    };

    /** @param nameAnnotation an annotation with a {@code String value()} that renames a component, or null for none */
    public Records(Class<? extends Annotation> nameAnnotation) {
        this.nameAnnotation = nameAnnotation;
    }

    /** The value as a {@code type}; null stays null (a primitive type gives its zero). */
    @SuppressWarnings("unchecked")
    public <T> T to(Class<T> type, Object value) {
        return (T) convert(type, value, "");
    }

    /** A list of maps as a list of records (or of any convertible element type). */
    public <T> List<T> toList(Class<T> element, Object list) {
        if (list == null) {
            return null;
        }
        if (!(list instanceof List<?> in)) {
            throw new Mismatch("a list needs an array, not " + kind(list));
        }
        List<T> out = new ArrayList<>(in.size());
        for (int i = 0; i < in.size(); i++) {
            out.add(element.cast(box(element, convert(element, in.get(i), "[" + i + "]"))));
        }
        return out;
    }

    /** The components of {@code record}, in declaration order, under their (annotated) names. Values are as the record holds them. */
    public Map<String, Object> fields(Record record) {
        Shape shape = shapes.get(record.getClass());
        Map<String, Object> m = new LinkedHashMap<>(shape.names.length * 2);
        for (int i = 0; i < shape.names.length; i++) {
            try {
                m.put(shape.names[i], shape.accessors[i].invoke(record));
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("cannot read " + record.getClass().getSimpleName() + "." + shape.components[i].getName(), e);
            }
        }
        return m;
    }

    /** A conversion failure that names the path to the value; not wrapped again on the way up. */
    public static final class Mismatch extends IllegalArgumentException {
        Mismatch(String message) {
            super(message);
        }

        Mismatch(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final class Shape {
        final RecordComponent[] components;
        final String[] names;
        final Method[] accessors;
        final Constructor<?> constructor;

        Shape(Class<?> type) {
            components = type.getRecordComponents();
            if (components == null) {
                throw new IllegalArgumentException(type.getName() + " is not a record");
            }
            names = new String[components.length];
            accessors = new Method[components.length];
            Class<?>[] types = new Class<?>[components.length];
            for (int i = 0; i < components.length; i++) {
                names[i] = nameOf(components[i]);
                accessors[i] = components[i].getAccessor();
                accessors[i].setAccessible(true);
                types[i] = components[i].getType();
            }
            try {
                constructor = type.getDeclaredConstructor(types);
                constructor.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException("no canonical constructor: " + type.getName(), e);
            }
        }

        private String nameOf(RecordComponent c) {
            if (nameAnnotation != null) {
                Annotation a = c.getAnnotation(nameAnnotation);
                if (a != null) {
                    try {
                        return (String) nameAnnotation.getMethod("value").invoke(a);
                    } catch (ReflectiveOperationException | ClassCastException e) {
                        throw new IllegalArgumentException(nameAnnotation.getName() + " needs a String value()", e);
                    }
                }
            }
            return c.getName();
        }
    }

    private static Object box(Class<?> type, Object v) {
        return v;
    }

    private Object convert(Type type, Object v, String path) {
        Class<?> raw;
        Type[] args = null;
        if (type instanceof Class<?> c) {
            raw = c;
        } else if (type instanceof ParameterizedType p) {
            raw = (Class<?>) p.getRawType();
            args = p.getActualTypeArguments();
        } else if (type instanceof GenericArrayType g) {
            return toArray(g.getGenericComponentType(), v, path);
        } else if (type instanceof WildcardType w) {
            return convert(w.getUpperBounds()[0], v, path);
        } else {
            return v;                                              // a type variable: nothing to go on, left as is
        }
        if (raw == Object.class) {
            return v;
        }
        if (raw == Optional.class) {
            return Optional.ofNullable(convert(args != null ? args[0] : Object.class, v, path));
        }
        if (v == null) {
            return raw.isPrimitive() ? Array.get(Array.newInstance(raw, 1), 0) : null;     // the primitive's zero, boxed
        }
        try {
            if (raw == String.class) {
                if (v instanceof Map || v instanceof Collection || v.getClass().isArray()) {
                    throw new IllegalArgumentException("a String needs a scalar, not " + kind(v));
                }
                return v instanceof String s ? s : v.toString();
            }
            if (raw == int.class || raw == Integer.class) {
                return v instanceof Number n ? n.intValue() : Integer.parseInt(v.toString());
            }
            if (raw == long.class || raw == Long.class) {
                return v instanceof Number n ? n.longValue() : Long.parseLong(v.toString());
            }
            if (raw == double.class || raw == Double.class) {
                return v instanceof Number n ? n.doubleValue() : Double.parseDouble(v.toString());
            }
            if (raw == float.class || raw == Float.class) {
                return v instanceof Number n ? n.floatValue() : Float.parseFloat(v.toString());
            }
            if (raw == boolean.class || raw == Boolean.class) {
                return v instanceof Boolean b ? b : Boolean.parseBoolean(v.toString());
            }
            if (raw == short.class || raw == Short.class) {
                return v instanceof Number n ? n.shortValue() : Short.parseShort(v.toString());
            }
            if (raw == byte.class || raw == Byte.class) {
                return v instanceof Number n ? n.byteValue() : Byte.parseByte(v.toString());
            }
            if (raw == char.class || raw == Character.class) {
                String s = v.toString();
                if (s.length() != 1) {
                    throw new IllegalArgumentException("not one character");
                }
                return s.charAt(0);
            }
            if (raw.isEnum()) {
                if (raw.isInstance(v)) {
                    return v;
                }
                String name = v.toString();
                for (Object c : raw.getEnumConstants()) {
                    if (((Enum<?>) c).name().equals(name)) {
                        return c;
                    }
                }
                for (Object c : raw.getEnumConstants()) {
                    if (((Enum<?>) c).name().equalsIgnoreCase(name)) {
                        return c;
                    }
                }
                throw new IllegalArgumentException("no constant " + name + " in " + raw.getSimpleName());
            }
            if (raw.isRecord()) {
                if (raw.isInstance(v)) {
                    return v;
                }
                if (!(v instanceof Map<?, ?> m)) {
                    throw new IllegalArgumentException("a " + raw.getSimpleName() + " needs an object, not " + kind(v));
                }
                Shape shape = shapes.get(raw);
                Object[] values = new Object[shape.components.length];
                for (int i = 0; i < values.length; i++) {
                    values[i] = convert(shape.components[i].getGenericType(), m.get(shape.names[i]), path.isEmpty() ? shape.names[i] : path + "." + shape.names[i]);
                }
                try {
                    return shape.constructor.newInstance(values);
                } catch (InvocationTargetException e) {
                    throw new IllegalArgumentException(raw.getSimpleName() + " refused its values: " + e.getCause(), e.getCause());
                } catch (ReflectiveOperationException e) {
                    throw new IllegalArgumentException("cannot build " + raw.getName(), e);
                }
            }
            if (raw.isArray()) {
                return toArray(raw.getComponentType(), v, path);
            }
            if (Collection.class.isAssignableFrom(raw)) {
                if (!(v instanceof Collection<?> in)) {
                    throw new IllegalArgumentException("a " + raw.getSimpleName() + " needs an array, not " + kind(v));
                }
                Type element = args != null ? args[0] : Object.class;
                Collection<Object> out = Set.class.isAssignableFrom(raw) ? new LinkedHashSet<>(in.size() * 2) : new ArrayList<>(in.size());
                int i = 0;
                for (Object o : in) {
                    out.add(convert(element, o, path + "[" + i++ + "]"));
                }
                return out;
            }
            if (Map.class.isAssignableFrom(raw)) {
                if (!(v instanceof Map<?, ?> in)) {
                    throw new IllegalArgumentException("a Map needs an object, not " + kind(v));
                }
                Type key = args != null ? args[0] : String.class, value = args != null ? args[1] : Object.class;
                Map<Object, Object> out = new LinkedHashMap<>(in.size() * 2);
                for (Map.Entry<?, ?> e : in.entrySet()) {
                    out.put(convert(key, e.getKey(), path + "{key}"), convert(value, e.getValue(), path.isEmpty() ? String.valueOf(e.getKey()) : path + "." + e.getKey()));
                }
                return out;
            }
            if (raw.isInstance(v)) {
                return v;                                          // a date, an id, a UUID, a byte[] the format decoded already
            }
            if (raw == Instant.class) {
                return v instanceof Number n ? Instant.ofEpochMilli(n.longValue()) : v instanceof java.util.Date d ? d.toInstant() : Instant.parse(v.toString());
            }
            if (raw == java.time.LocalDate.class) {
                return java.time.LocalDate.parse(v.toString());
            }
            if (raw == java.time.LocalDateTime.class) {
                return java.time.LocalDateTime.parse(v.toString());
            }
            if (raw == java.time.LocalTime.class) {
                return java.time.LocalTime.parse(v.toString());
            }
            if (raw == java.time.OffsetDateTime.class) {
                return java.time.OffsetDateTime.parse(v.toString());
            }
            if (raw == java.time.ZonedDateTime.class) {
                return java.time.ZonedDateTime.parse(v.toString());
            }
            if (raw == java.time.Duration.class) {
                return java.time.Duration.parse(v.toString());
            }
            if (raw == java.util.Date.class) {
                return java.util.Date.from(v instanceof Number n ? Instant.ofEpochMilli(n.longValue()) : Instant.parse(v.toString()));
            }
            if (raw == BigDecimal.class) {
                return v instanceof BigInteger b ? new BigDecimal(b) : new BigDecimal(v.toString());
            }
            if (raw == BigInteger.class) {
                return v instanceof BigDecimal d ? d.toBigInteger() : v instanceof Double d ? BigDecimal.valueOf(d).toBigInteger() : new BigInteger(v.toString());
            }
            if (raw == UUID.class) {
                return UUID.fromString(v.toString());
            }
            if (raw == Number.class) {
                throw new IllegalArgumentException("not a number");
            }
        } catch (Mismatch nested) {
            throw nested;                                          // already says where
        } catch (IllegalArgumentException | DateTimeException e) {
            throw new Mismatch("at " + (path.isEmpty() ? "the value" : path) + ": " + e.getMessage() + " (" + kind(v) + " to " + raw.getSimpleName() + ")", e);
        }
        throw new Mismatch("at " + (path.isEmpty() ? "the value" : path) + ": cannot turn " + kind(v) + " into " + raw.getName());
    }

    private Object toArray(Type element, Object v, String path) {
        if (v == null) {
            return null;
        }
        Class<?> raw = element instanceof Class<?> c ? c : element instanceof ParameterizedType p ? (Class<?>) p.getRawType() : Object.class;
        if (raw == byte.class) {
            if (v instanceof byte[] b) {
                return b;
            }
            if (!(v instanceof String s)) {
                throw new Mismatch("at " + path + ": byte[] is read from bytes or a base64 string, not " + kind(v));
            }
            return Base64.getDecoder().decode(s);
        }
        if (v.getClass().isArray() && raw.isAssignableFrom(v.getClass().getComponentType())) {
            return v;
        }
        if (!(v instanceof Collection<?> in)) {
            throw new Mismatch("at " + path + ": an array needs an array, not " + kind(v));
        }
        Object out = Array.newInstance(raw, in.size());
        int i = 0;
        for (Object o : in) {
            Array.set(out, i, convert(element, o, path + "[" + i + "]"));
            i++;
        }
        return out;
    }

    private static String kind(Object v) {
        return v == null ? "null" : v instanceof Map ? "an object" : v instanceof Collection ? "an array" : v instanceof String ? "a string" : v instanceof Boolean ? "a boolean"
                : v instanceof Number ? "a number" : v.getClass().getSimpleName();
    }
}
