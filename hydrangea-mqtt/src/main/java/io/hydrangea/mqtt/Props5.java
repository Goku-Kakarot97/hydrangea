package io.hydrangea.mqtt;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/** The properties block of an MQTT 5 packet: every property the specification defines, as plain fields (null = absent). */
final class Props5 {

    Integer payloadFormat;
    Long messageExpiry;
    String contentType;
    String responseTopic;
    byte[] correlationData;
    final List<Integer> subscriptionIds = new ArrayList<>();
    Long sessionExpiry;
    String assignedClientId;
    Integer serverKeepAlive;
    String authMethod;
    byte[] authData;
    Integer requestProblemInfo;
    Long willDelay;
    Integer requestResponseInfo;
    String responseInfo;
    String serverReference;
    String reasonString;
    Integer receiveMaximum;
    Integer topicAliasMaximum;
    Integer topicAlias;
    Integer maximumQoS;
    Integer retainAvailable;
    Long maximumPacketSize;
    Integer wildcardAvailable;
    Integer subIdsAvailable;
    Integer sharedAvailable;
    final List<UserProperty> user = new ArrayList<>();

    static Props5 of(MqttProperties p) {
        Props5 o = new Props5();
        if (p == null) {
            return o;
        }
        o.payloadFormat = p.payloadFormat();
        o.messageExpiry = p.messageExpirySeconds();
        o.contentType = p.contentType();
        o.responseTopic = p.responseTopic();
        o.correlationData = p.correlationData();
        o.user.addAll(p.userProperties());
        return o;
    }

    MqttProperties toPublic() {
        if (payloadFormat == null && messageExpiry == null && contentType == null && responseTopic == null && correlationData == null
                && user.isEmpty() && subscriptionIds.isEmpty()) {
            return MqttProperties.NONE;
        }
        return new MqttProperties(payloadFormat, messageExpiry, contentType, responseTopic, correlationData, user, subscriptionIds);
    }

    /** Reads a length-prefixed properties block. Anything the specification forbids (an unknown identifier, a repeat of a single-use property, a value out of range) is a protocol error. */
    static Props5 read(Packets.Reader r) {
        int len = r.vbi();
        Packets.Reader b = r.sub(len);
        Props5 p = new Props5();
        BitSet seen = new BitSet();
        while (b.remaining() > 0) {
            int id = b.vbi();
            boolean repeatable = id == 0x0B || id == 0x26;
            if (!repeatable && id < 64) {
                if (seen.get(id)) {
                    throw new MqttException("property 0x" + Integer.toHexString(id) + " appears twice", false);
                }
                seen.set(id);
            }
            switch (id) {
                case 0x01 -> p.payloadFormat = bool(b.u8(), id);
                case 0x02 -> p.messageExpiry = b.u32();
                case 0x03 -> p.contentType = b.str();
                case 0x08 -> p.responseTopic = b.str();
                case 0x09 -> p.correlationData = b.bin();
                case 0x0B -> {
                    int v = b.vbi();
                    if (v == 0) throw new MqttException("subscription identifier 0", false);
                    p.subscriptionIds.add(v);
                }
                case 0x11 -> p.sessionExpiry = b.u32();
                case 0x12 -> p.assignedClientId = b.str();
                case 0x13 -> p.serverKeepAlive = b.u16();
                case 0x15 -> p.authMethod = b.str();
                case 0x16 -> p.authData = b.bin();
                case 0x17 -> p.requestProblemInfo = bool(b.u8(), id);
                case 0x18 -> p.willDelay = b.u32();
                case 0x19 -> p.requestResponseInfo = bool(b.u8(), id);
                case 0x1A -> p.responseInfo = b.str();
                case 0x1C -> p.serverReference = b.str();
                case 0x1F -> p.reasonString = b.str();
                case 0x21 -> {
                    p.receiveMaximum = b.u16();
                    if (p.receiveMaximum == 0) throw new MqttException("receive maximum 0", false);
                }
                case 0x22 -> p.topicAliasMaximum = b.u16();
                case 0x23 -> {
                    p.topicAlias = b.u16();
                    if (p.topicAlias == 0) throw new MqttException("topic alias 0", false);
                }
                case 0x24 -> {
                    p.maximumQoS = b.u8();
                    if (p.maximumQoS > 1) throw new MqttException("maximum QoS " + p.maximumQoS, false);
                }
                case 0x25 -> p.retainAvailable = bool(b.u8(), id);
                case 0x26 -> {
                    String n = b.str();
                    p.user.add(new UserProperty(n, b.str()));
                }
                case 0x27 -> {
                    p.maximumPacketSize = b.u32();
                    if (p.maximumPacketSize == 0) throw new MqttException("maximum packet size 0", false);
                }
                case 0x28 -> p.wildcardAvailable = bool(b.u8(), id);
                case 0x29 -> p.subIdsAvailable = bool(b.u8(), id);
                case 0x2A -> p.sharedAvailable = bool(b.u8(), id);
                default -> throw new MqttException("unknown property 0x" + Integer.toHexString(id), false);
            }
        }
        return p;
    }

    private static int bool(int v, int id) {
        if (v > 1) {
            throw new MqttException("property 0x" + Integer.toHexString(id) + " is " + v + ", not 0 or 1", false);
        }
        return v;
    }

    /** All fields that are set, without the length prefix (what {@link Packets} puts after a variable byte integer holding the size). The client only sets those a client may send; tests play the broker with the rest. */
    byte[] encode() {
        Packets.Writer w = new Packets.Writer(32);
        if (payloadFormat != null) w.u8(0x01).u8(payloadFormat);
        if (messageExpiry != null) w.u8(0x02).u32(messageExpiry);
        if (contentType != null) w.u8(0x03).str(Packets.utf8(contentType));
        if (responseTopic != null) {
            Packets.checkTopicName(responseTopic);
            w.u8(0x08).str(Packets.utf8(responseTopic));
        }
        if (correlationData != null) w.u8(0x09).bin(correlationData);
        for (int id : subscriptionIds) w.u8(0x0B).vbi(id);
        if (sessionExpiry != null) w.u8(0x11).u32(sessionExpiry);
        if (assignedClientId != null) w.u8(0x12).str(Packets.utf8(assignedClientId));
        if (serverKeepAlive != null) w.u8(0x13).u16(serverKeepAlive);
        if (authMethod != null) w.u8(0x15).str(Packets.utf8(authMethod));
        if (authData != null) w.u8(0x16).bin(authData);
        if (requestProblemInfo != null) w.u8(0x17).u8(requestProblemInfo);
        if (willDelay != null) w.u8(0x18).u32(willDelay);
        if (requestResponseInfo != null) w.u8(0x19).u8(requestResponseInfo);
        if (responseInfo != null) w.u8(0x1A).str(Packets.utf8(responseInfo));
        if (serverReference != null) w.u8(0x1C).str(Packets.utf8(serverReference));
        if (reasonString != null) w.u8(0x1F).str(Packets.utf8(reasonString));
        if (receiveMaximum != null) w.u8(0x21).u16(receiveMaximum);
        if (topicAliasMaximum != null) w.u8(0x22).u16(topicAliasMaximum);
        if (topicAlias != null) w.u8(0x23).u16(topicAlias);
        if (maximumQoS != null) w.u8(0x24).u8(maximumQoS);
        if (retainAvailable != null) w.u8(0x25).u8(retainAvailable);
        for (UserProperty u : user) w.u8(0x26).str(Packets.utf8(u.name())).str(Packets.utf8(u.value()));
        if (maximumPacketSize != null) w.u8(0x27).u32(maximumPacketSize);
        if (wildcardAvailable != null) w.u8(0x28).u8(wildcardAvailable);
        if (subIdsAvailable != null) w.u8(0x29).u8(subIdsAvailable);
        if (sharedAvailable != null) w.u8(0x2A).u8(sharedAvailable);
        return w.toByteArray();
    }

    MqttServerInfo toServerInfo(MqttServerInfo defaults) {
        return new MqttServerInfo(
                receiveMaximum != null ? receiveMaximum : defaults.receiveMaximum(),
                maximumQoS != null ? maximumQoS : defaults.maximumQoS(),
                retainAvailable != null ? retainAvailable == 1 : defaults.retainAvailable(),
                maximumPacketSize != null ? maximumPacketSize : defaults.maximumPacketSize(),
                topicAliasMaximum != null ? topicAliasMaximum : defaults.topicAliasMaximum(),
                wildcardAvailable != null ? wildcardAvailable == 1 : defaults.wildcardSubscriptionAvailable(),
                subIdsAvailable != null ? subIdsAvailable == 1 : defaults.subscriptionIdentifiersAvailable(),
                sharedAvailable != null ? sharedAvailable == 1 : defaults.sharedSubscriptionAvailable(),
                assignedClientId,
                serverKeepAlive != null ? serverKeepAlive : defaults.serverKeepAlive(),
                responseInfo,
                sessionExpiry != null ? sessionExpiry : defaults.sessionExpirySeconds());
    }
}
