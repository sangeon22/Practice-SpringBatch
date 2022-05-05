package com.example.spring.batch.part6;

import com.example.spring.batch.part4.UserRepository;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

// Partition Step을 위한 설정
public class UserLevelUpPartitioner implements Partitioner {

    private final UserRepository userRepository;

    public UserLevelUpPartitioner(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    // gridSize가 슬레이브 스텝 사이즈
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        // 파티셔닝할 대상의 가장 작은 Id값, 가장 큰 Id값을 구한다.
        long minId = userRepository.findMinId(); // 1번
        long maxId = userRepository.findMaxId(); // 40,000번

        // 각 슬레이브 스텝에서 처리할 아이템의 갯수
        long targetSize = (maxId - minId) / gridSize + 1; // 5000

        /**
         * partition0 : 1, 5000
         * partition1 : 5001, 10000
         * ...
         * partition7 : 35001, 40000
         * 위처럼 처리되도록 값이 ExecutionContext에 저장
         */
        Map<String, ExecutionContext> result = new HashMap<>();

        // 스텝의 번호
        long number = 0;

        // 아이템의 시작번호
        long start = minId;

        // 아이템의 끝번호
        long end = start + targetSize - 1;

        while (start <= maxId) {
            ExecutionContext value = new ExecutionContext();

            result.put("partition" + number, value);

            if (end >= maxId) {
                end = maxId;
            }

            value.putLong("minId", start);
            value.putLong("maxId", end);

            start += targetSize;
            end += targetSize;
            number++;
        }

        return result;
    }


}
