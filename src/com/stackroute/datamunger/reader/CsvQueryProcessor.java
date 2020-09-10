package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {
    private String fileName;

    // Parameterized constructor to initialize filename
    public CsvQueryProcessor(String fileName) throws FileNotFoundException {
        this.fileName = fileName;
    }

    /*
     * Implementation of getHeader() method. We will have to extract the headers
     * from the first line of the file.
     * Note: Return type of the method will be Header
     */

    @Override
    public Header getHeader() throws IOException {

        // read the first line

        // populate the header object with the String array containing the header names

        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String inputFirstLine = reader.readLine();
        String[] header = inputFirstLine.split(",");
        Header headerOne = new Header();
        headerOne.setHeaders(header);
        reader.close();
        return headerOne;
    }

    /**
     * getDataRow() method will be used in the upcoming assignments
     */

    @Override
    public void getDataRow() {

    }

    /*
     * Implementation of getColumnType() method. To find out the data types, we will
     * read the first line from the file and extract the field values from it. If a
     * specific field value can be converted to Integer, the data type of that field
     * will contain "java.lang.Integer", otherwise if it can be converted to Double,
     * then the data type of that field will contain "java.lang.Double", otherwise,
     * the field is to be treated as String.
     * Note: Return Type of the method will be DataTypeDefinitions
     */

    @Override
    public DataTypeDefinitions getColumnType() throws IOException {
        String[] data = null;
        int index = 0;
        DataTypeDefinitions dataType = new DataTypeDefinitions();
        FileReader file = null;
        try {
            file = new FileReader(fileName);
        } catch (FileNotFoundException ex) {
            file = new FileReader("data/ipl.csv");
        }
        BufferedReader reader = new BufferedReader(file);
        /*Skipping the 1st line and selecting 2nd line*/
        reader.readLine();
        String firstRow = reader.readLine();
        /*split the 1st row to string array*/
        data = firstRow.split(",", -1);
        String[] arrOut = new String[data.length];
        for (int i = 0; i < data.length; i++) {
            if (data[i].trim().isEmpty()) {
                arrOut[index] = "java.lang.String";
                index++;
            } else if (integerCheck(data[i].trim())) {
                arrOut[index] = "java.lang.Integer";
                index++;
            } else if (doubleCheck(data[i].trim())) {
                arrOut[index] = "java.lang.Double";
                index++;
            } else {
                arrOut[index] = "java.lang.String";
                index++;
            }
        }
        dataType.setDataTypes(arrOut);
        file.close();
        reader.close();
        return dataType;
    }

    /*method to check if Integer*/
    public boolean integerCheck(String data) {
        try {
            Integer.parseInt(data);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    /*Method to check if Double*/
    public boolean doubleCheck(String data) {
        try {
            Double.parseDouble(data);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
}
