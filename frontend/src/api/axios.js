import axios from 'axios';

const api = axios.create({
    baseURL: import.meta.env.VITE_API_URL || 'http://localhost:3000',    // URL: For Backend API
    withCredentials: true,
    headers: {
        'Content-Type': 'application/json'
    }
});

// Response interceptor
api.interceptors.response.use(
    (response) => response,
    async (error) => {
        const originalRequest = error.config;

        if (
            error.response?.status === 401 &&
            !originalRequest._retry &&
            !originalRequest.url?.includes('/api/auth/refresh') &&
            !originalRequest.url?.includes('/api/auth/login') &&
            !originalRequest.url?.includes('/api/auth/me')
        ) {
            originalRequest._retry = true;

            try {
                await api.post('/api/auth/refresh');
                return api(originalRequest);
            } catch (refreshError) {
                if (
                    window.location.pathname !== '/login' &&
                    window.location.pathname !== '/register' &&
                    window.location.pathname !== '/'
                ) {
                    window.location.href = '/login';
                }
                return Promise.reject(refreshError);
            }
        }

        return Promise.reject(error);
    }
);


export default api;