package com.example.cs.Gecko;

import com.example.cs.Gecko.rfq.decorator.RfqDecoratorMain;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import java.util.concurrent.TimeUnit;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;

import static com.example.cs.Gecko.rfq.utils.ChatterboxServer.runSender;

@RestController
public class GeckoRestController {

    @GetMapping("/hello")
    String sayHello(){
        return "Hello there";
    }


    @PostMapping("/submitRfq")
    String getRfqEnrichment(@RequestBody String rfqData) throws Exception {
        runSender(rfqData);
        TimeUnit.SECONDS.sleep(3);
        String returnDat;
        File file = new File("C:\\exercises\\cs-2021-eu-team-3\\src\\main\\resources\\rfqJSONFormat.json");
        try(Stream<String> linesStream = Files.lines(file.toPath())){
            returnDat = linesStream.reduce("", (a,b) -> a + b);
        }
        return returnDat;
    }
}
