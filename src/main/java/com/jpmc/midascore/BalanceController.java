package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.foundation.Balance;
import org.springframework.web.bind.annotation.*;

@RestController
public class BalanceController {

    private final UserRepository userRepository;

    public BalanceController(UserRepository userRepository) {
        this.userRepository = userRepository;
    }
    @GetMapping("/balance")
    public Balance getBalance(@RequestParam Long userId) {

        UserRecord user = userRepository.findById(userId).orElse(null);

        float balance = (user != null) ? user.getBalance() : 0f;

        return new Balance(balance);
    }
}