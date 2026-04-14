package public

import (
	"context"

	"decentralized-api/observability"

	"github.com/labstack/echo/v4"
)

func startObservabilityInferenceRequestContext(ctx echo.Context) (context.Context, *observability.Operation) {
	requestContext := observability.Inference.ExtractRequestContext(ctx.Request().Context(), ctx.Request().Header)
	ctx.SetRequest(ctx.Request().WithContext(requestContext))
	requestContext, requestOp := observability.Inference.StartRequest(requestContext, ctx.Request().Method)
	ctx.SetRequest(ctx.Request().WithContext(requestContext))
	return requestContext, requestOp
}