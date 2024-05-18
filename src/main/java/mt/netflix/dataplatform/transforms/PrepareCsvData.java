package mt.netflix.dataplatform.transforms;

import org.apache.beam.sdk.transforms.DoFn;

public class PrepareCsvData extends DoFn<String[], String> {

    @ProcessElement
    public void ProcessElement(@Element String[] element, OutputReceiver<String> receiver){
        StringBuilder csvString = new StringBuilder();

        for(int i=0; i< element.length; i++){
            if( !(i == 1) ){
                if (i == 7){

                    csvString.append(addQuotes(element[i].split(",")[0])).append(",");
                }
                else {
                    csvString.append(addQuotes(element[i])).append(",");
                }
            }
        }
        if (csvString.length() > 0) {
            csvString.deleteCharAt(csvString.length() - 1);
        }
//        System.out.println(csvString);
        receiver.output(String.valueOf(csvString));
    }

    public static String addQuotes(String value) {
        return "\"" + value + "\"";
    }
}
