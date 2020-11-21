package deerBase;

import java.text.ParseException;

import deerBase.StringField;

import java.io.*;

/**
 * Class representing a type in DeerBase.
 * Types are static objects defined by this class; hence, the Type
 * constructor is private.
 */
public enum Type implements Serializable {
    INT_TYPE() {
        @Override
        public int getLen() {
            return 4;
        }

        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                return new IntField(dis.readInt());
            }  catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }

    }, STRING_TYPE() {
        @Override
        public int getLen() {
            return STRING_LEN+4;
        }
        
        /*
        * @see StringField serialize() to understand the process of parse
        * how to write a StringField object into disk
        */
        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                int strLen = dis.readInt();
                byte buf[] = new byte[strLen];
                dis.read(buf);
                // skip padding 0s, 
                dis.skipBytes(STRING_LEN-strLen);
                return new StringField(new String(buf), STRING_LEN);
            } catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }
    };
    
    public static final int STRING_LEN = 128;

  /**
   * @return the number of bytes required to store a field of this type.
   */
    public abstract int getLen();

  /**
   * @return a Field object of the same type as this object that has contents
   *   read from the specified DataInputStream.
   * @param dis The input stream to read from
   * @throws ParseException if the data read from the input stream is not
   *   of the appropriate type.
   */
    public abstract Field parse(DataInputStream dis) throws ParseException;

}
