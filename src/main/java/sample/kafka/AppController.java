package sample.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
@Validated
public class AppController {

    @Autowired
    private Producer producer;

    private static final Logger LOG = LoggerFactory.getLogger(AppController.class.getName());

    @GetMapping(value = "/send-message")
    public ResponseEntity<?> send(
            @RequestHeader Map<String, String> headers,
            @RequestParam String message
    ) {
        headers.forEach((key, value) -> LOG.info(String.format("Header '%s' = %s", key, value)));
        {
            LOG.info("Requested /api/v1/send-message");
            LOG.info("Message: {}", message);

            try {
                this.producer.send(message);
                return ResponseEntity.ok().build();
            } catch (Exception ex) {
                UUID uuid = UUID.randomUUID();
                LOG.error(uuid.toString().concat(" | ").concat(String.valueOf(ex)));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(uuid);
            }
        }
    }
}
