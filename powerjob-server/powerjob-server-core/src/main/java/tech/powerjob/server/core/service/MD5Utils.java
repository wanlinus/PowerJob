package tech.powerjob.server.core.service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Utils {

    public static void main(String[] args) throws NoSuchAlgorithmException {
        System.out.println(toMD5("Topevery_Ai_123QQWEdffAi_1629683278095"));
    }

    public static String toMD5(String src) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        messageDigest.update(src.getBytes());
        byte[] digest = messageDigest.digest();
        return encodeHex(digest);
    }

    private static String encodeHex(byte[] bytes) {
        StringBuilder stringBuilder = new StringBuilder();
        for(byte bin : bytes) {
            int result = bin & 0xFF;
            if(result < 0x10) {
                stringBuilder.append("0");
            }
            stringBuilder.append(Integer.toHexString(result));
        }
        return stringBuilder.toString();
    }
}
