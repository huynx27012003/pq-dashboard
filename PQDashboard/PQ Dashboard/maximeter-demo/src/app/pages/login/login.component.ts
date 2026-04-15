import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router, ActivatedRoute } from '@angular/router';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { AuthService } from '../../services/auth.service';

@Component({
    selector: 'app-login',
    standalone: true,
    imports: [
        CommonModule,
        ReactiveFormsModule,
        MatCardModule,
        MatFormFieldModule,
        MatInputModule,
        MatButtonModule,
        MatIconModule,
        MatProgressSpinnerModule,
    ],
    templateUrl: './login.component.html',
    styleUrl: './login.component.scss',
})
export class LoginComponent {
    private fb = inject(FormBuilder);
    private router = inject(Router);
    private route = inject(ActivatedRoute);
    private authService = inject(AuthService);

    loginForm: FormGroup = this.fb.group({
        username: ['', Validators.required],
        password: ['', Validators.required],
    });

    isLoading = false;
    hidePassword = true;
    errorMessage = '';
    returnUrl = '/dashboard';

    constructor() {
        this.returnUrl = this.route.snapshot.queryParams['returnUrl'] || '/dashboard';

        if (this.authService.isLoggedIn) {
            this.router.navigate([this.returnUrl]);
        }
    }

    onSubmit(): void {
        if (this.loginForm.invalid) {
            return;
        }

        this.isLoading = true;
        this.errorMessage = '';

        this.authService.login(this.loginForm.value).subscribe({
            next: (response) => {
                this.isLoading = false;
                if (response.success) {
                    this.router.navigate([this.returnUrl]);
                } else {
                    this.errorMessage = response.message || 'Đăng nhập thất bại';
                }
            },
            error: () => {
                this.isLoading = false;
                this.errorMessage = 'Có lỗi xảy ra. Vui lòng thử lại.';
            },
        });
    }
}
