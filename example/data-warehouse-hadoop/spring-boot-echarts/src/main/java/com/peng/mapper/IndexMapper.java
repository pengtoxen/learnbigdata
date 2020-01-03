package com.peng.mapper;

import com.peng.model.Index;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;


@Mapper
public interface IndexMapper {

    List<Index> findAll();
}
