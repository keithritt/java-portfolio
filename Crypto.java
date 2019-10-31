package jutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Crypto {
	// based on sample code from: https://howtodoinjava.com/security/java-aes-encryption-example/

    private static SecretKeySpec secretKey;
    private static byte[] key;
    private String privateKey;
    private String privateFileName = "private.json";
    private File privateFile;
    private JSONObject privateConfigs;

    public static void main(String[] args){
    	Crypto myCrypto = new Crypto();
    	myCrypto.runWizard();
    }

    public Crypto() {
    	this.privateFile = new File(this.privateFileName);

    	JSONParser jsonParser = new JSONParser();
    	if(this.privateFile.exists()){
			try {
				this.privateConfigs = (JSONObject) jsonParser.parse(new FileReader(this.privateFile));
	    		this.privateKey = (String) this.privateConfigs.get("private-key");
	    		this.privateKey = Crypto.decrypt(this.privateKey, "secret");
			} catch (IOException | org.json.simple.parser.ParseException e) {
				e.printStackTrace();
			}
    	}
    	else
    		this.privateConfigs = new JSONObject();
    }

    public static void setKey(String myKey)
    {
        MessageDigest sha = null;
        try {
            key = myKey.getBytes("UTF-8");
            sha = MessageDigest.getInstance("SHA-1");
            key = sha.digest(key);
            key = Arrays.copyOf(key, 16);
            secretKey = new SecretKeySpec(key, "AES");
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

	public String getConfig(String name){
		String config = (String) this.privateConfigs.get(name);
		config = Crypto.decrypt(config, this.privateKey);
		return config;
	}

	public void setConfig(String name, String value){
		Debug.pr("setConfig("+name+", "+value+")");
	}

	//replace middle of string with * characters
	public static String showPartial(String msg){
		if(msg == null)
			return null;
		Integer msgLength = msg.length();
		String partial = "";
		if(msgLength < 3)
			return "**";

		for(int i = 0; i < msgLength; i++){
			if(i == 0 || i == msgLength -1)
				partial = partial + msg.charAt(i);
			else
				partial = partial + "*";
		}
		return partial;
	}

    public static String encrypt(String strToEncrypt, String secret)
    {
        try
        {
        	setKey(secret);
        	Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            return Base64.getEncoder().encodeToString(cipher.doFinal(strToEncrypt.getBytes("UTF-8")));
        }
        catch (Exception e)
        {
            System.out.println("Error while encrypting: " + e.toString());
        }
        return null;
    }

    public static String decrypt(String strToDecrypt, String secret)
    {
    	if(strToDecrypt == null)
    		return null;
        try
        {
        	setKey(secret);
        	Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            return new String(cipher.doFinal(Base64.getDecoder().decode(strToDecrypt)));
        }
        catch (Exception e)
        {
            System.out.println("Error while decrypting: " + e.toString());
        }
        return null;
    }
}