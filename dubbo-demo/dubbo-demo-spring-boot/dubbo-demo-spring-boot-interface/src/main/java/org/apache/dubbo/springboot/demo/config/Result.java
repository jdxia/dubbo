package org.apache.dubbo.springboot.demo.config;

import java.io.Serializable;

public class Result<T> implements Serializable {

    private Long serialVersionUID = 3432423423L;

    private int code;
    private String message;
    private T data;

    // 构造方法、getter和setter方法省略
    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public static <T> Result<T> success(T data) {
        Result<T> result = new Result<>();
        result.setCode(200);
        result.setMessage("success");
        result.setData(data);
        return result;
    }

    public static <T> Result<T> error(int code, String message) {
        Result<T> result = new Result<>();
        result.setCode(code);
        result.setMessage(message);
        return result;
    }

    public boolean isSuccess() {
        return this.code == 200;
    }

    public boolean isError() {
        return !isSuccess();
    }


}
